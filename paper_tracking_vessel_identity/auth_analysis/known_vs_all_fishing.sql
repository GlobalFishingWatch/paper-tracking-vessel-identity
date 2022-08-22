##########################################################################
--------------------------------------------------------------------------
-- Select a parameter block below for your analysis and uncomment the rows
--------------------------------------------------------------------------

#################################
## For all fishing vessels in AIS
#################################
CREATE TEMP FUNCTION res_factor() AS (5);
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2021-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2022-01-01");

##########################################################################


-----------------------------------------------------------------------
-- Assign gridcode to each cell in 100th degree to attach EEZ/RFMO info
-----------------------------------------------------------------------
CREATE TEMP FUNCTION assign_gridcode(lon FLOAT64, lat FLOAT64) AS (
  FORMAT ("lon:%+07.2f_lat:%+07.2f",
    ROUND (lon * 100.) / 100.,
    ROUND (lat * 100.) / 100.)
);


WITH
  identity_core_data AS (
    SELECT *
    FROM `world-fishing-827.vessel_identity.identity_core_v20220701`
  ),

  identity_authorization_data AS (
    SELECT *
    FROM `world-fishing-827.vessel_identity.identity_authorization_v20220701`
  ),

  -----------------------------------------------------------------
  -- Extract fishing vessels for 2012-2020 from the GFW public data
  -- in which an MMSI is associated with a fishing vessel
  -----------------------------------------------------------------
  fishing_vessels_2012_2020 AS (
    SELECT DISTINCT
      CAST (SPLIT (year, "fishing_hours_")[OFFSET(1)] AS INT64) AS year,
      mmsi AS ssvid,
      vessel_class_gfw AS best_vessel_class,
      flag_gfw AS best_flag
    FROM `global-fishing-watch.gfw_public_data.fishing_vessels_v2`
    UNPIVOT (
      fishing_hours_by_year
      FOR year IN (
        fishing_hours_2012,
        fishing_hours_2013,
        fishing_hours_2014,
        fishing_hours_2015,
        fishing_hours_2016,
        fishing_hours_2017,
        fishing_hours_2018,
        fishing_hours_2019,
        fishing_hours_2020 ))
    WHERE fishing_hours_by_year IS NOT NULL
  ),

  ------------------------------------------
  -- Extract fishing vessels for 2021
  -- from the internal GFW vessel_info table
  ------------------------------------------
  fishing_vessels_2021 AS (
    SELECT DISTINCT
      2021 AS year,
      ssvid,
      best.best_vessel_class,
      best.best_flag
    FROM `gfw_research.vi_ssvid_v20220601`
    WHERE on_fishing_list_best
      AND activity.last_timestamp >= "2021-01-01"
  ),

  -----------------------------------------------------------------------
  -- Pull all fishing vessels on AIS that are active in years of question
  -----------------------------------------------------------------------
  on_ais AS (
    SELECT DISTINCT year, ssvid, best_vessel_class, best_flag
    FROM (
      SELECT *
      FROM fishing_vessels_2012_2020
      UNION ALL
      SELECT *
      FROM fishing_vessels_2021 )
    WHERE year IN
      UNNEST (
        GENERATE_ARRAY (
          EXTRACT (YEAR FROM start_date()),
          EXTRACT (YEAR FROM end_date()),
          1))
  ),

  ---------------------------------------------
  -- Pull fishing vessel identity data
  -- that is active within the given time range
  ---------------------------------------------
  on_identity_db AS (
    SELECT DISTINCT ssvid, first_timestamp, last_timestamp
    FROM identity_core_data
    WHERE is_fishing
      AND (first_timestamp < end_date()
        AND last_timestamp >= start_date())
  ),

  --------------------------------
  -- Identity known vessels on AIS
  --------------------------------
  known_vessels AS (
    SELECT DISTINCT ssvid, first_timestamp, last_timestamp
    FROM on_ais AS a
    JOIN on_identity_db AS b
    USING (ssvid)
  ),

  ---------------------
  -- Raw fishing effort
  ---------------------
  fishing_effort_raw AS (
    SELECT *
    FROM (
      SELECT *
      FROM (
        -----------------------------------------------------
        -- Fishing effort in 2012-2020 from the GFW public data
        -----------------------------------------------------
        SELECT DISTINCT
          mmsi AS ssvid, date, cell_ll_lat AS lat, cell_ll_lon AS lon, fishing_hours,
          assign_gridcode (cell_ll_lon, cell_ll_lat) AS gridcode
        FROM `global-fishing-watch.gfw_public_data.fishing_effort_byvessel_v2`

        UNION ALL

        -----------------------------------------------------------------------
        -- Fishing effort in 2021 from the GWF internal pipeline
        -- This needs to be preliminarily queried and saved as a separate table
        -----------------------------------------------------------------------
        SELECT DISTINCT
          ssvid, date, lat, lon,
          SUM (IF (fishing_score > 0.5, hours, 0)) AS fishing_hours,
          assign_gridcode (lon, lat) AS gridcode
        FROM (
          SELECT
            ssvid, DATE (timestamp) AS date,
            FLOOR (lat * 100.) / 100. AS lat,
            FLOOR (lon * 100.) / 100. AS lon,
            hours,
            IF (best_vessel_class = "squid_jigger", night_loitering, nnet_score) AS fishing_score
          FROM `gfw_research.pipe_v20201001_fishing`
          LEFT JOIN fishing_vessels_2021
          USING (ssvid)
          WHERE timestamp BETWEEN "2021-01-01" AND "2021-12-31"
            AND seg_id IN (
              SELECT seg_id
              FROM `gfw_research.pipe_v20201001_segs`
              WHERE good_seg
                AND NOT overlapping_and_short ) )
        GROUP BY 1,2,3,4 )
      WHERE date BETWEEN DATE (start_date()) AND DATE (end_date()) )
  ),

  -------------------------------------------------
  -- Filter fishing effort for those vessels in AIS
  -------------------------------------------------
  fishing_effort AS (
    SELECT DISTINCT
      ssvid, lat, lon, date, fishing_hours
    FROM fishing_effort_raw
    JOIN on_ais
    USING (ssvid)
    WHERE year = EXTRACT (YEAR FROM date)
  ),

  ------------------------------------------------------------
  -- Make binned fishing effort rasters for all vessels in AIS
  ------------------------------------------------------------
  fishing_effort_binned_all_vessels AS (
    SELECT
      FLOOR (lat * res_factor()) AS lat_bin,
      FLOOR (lon * res_factor()) AS lon_bin,
      SUM (fishing_hours) AS fishing_hours
    FROM fishing_effort
    GROUP BY 1,2
  ),

  ----------------------------------------------------------
  -- Fishing effort for the authorization identified vessels
  ----------------------------------------------------------
  fishing_effort_known_vessels AS (
    SELECT DISTINCT
      a.ssvid, lat, lon, date, fishing_hours
    FROM fishing_effort AS a
    JOIN known_vessels AS b
    ON a.ssvid = b.ssvid
      AND TIMESTAMP (date) BETWEEN b.first_timestamp AND b.last_timestamp
  ),

  ------------------------------------------------------------
  -- Make binned fishing effort rasters for authorized vessels
  ------------------------------------------------------------
  fishing_effort_binned_known_vessels AS (
    SELECT
      lat_bin, lon_bin,
      a.fishing_hours AS fishing_hours_all,
      IFNULL (fishing_hours_known_vessels, 0) AS fishing_hours_known_vessels
    FROM (
      SELECT
        lat_bin, lon_bin, fishing_hours
      FROM fishing_effort_binned_all_vessels ) AS a
    LEFT JOIN (
      SELECT
        FLOOR (lat * res_factor()) AS lat_bin,
        FLOOR (lon * res_factor()) AS lon_bin,
        SUM (fishing_hours) AS fishing_hours_known_vessels
      FROM fishing_effort_known_vessels
      GROUP BY 1,2 ) AS b
    USING (lat_bin, lon_bin)
  )


#########################################################################
-------------------------------------------------------------------------
-- OPTIONS FOR OUTPUT: You may want to choose one final query block below
-- to produce output in which you are interested
-------------------------------------------------------------------------


##################################################################
## OPTION 1: Gridded fishing effort for all fishing vessels in AIS
##################################################################
 SELECT * FROM fishing_effort_binned_all_vessels

####################################################################
## OPTION 2: Gridded fishing effort for all identified/known vessels
####################################################################
-- SELECT * FROM fishing_effort_binned_known_vessels


#########################################################################