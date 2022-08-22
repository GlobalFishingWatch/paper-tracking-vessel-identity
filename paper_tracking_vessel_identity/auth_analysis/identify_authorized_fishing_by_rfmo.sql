--------------------------------------------------------------------------
-- Select a parameter block below for your analysis and uncomment the rows
--------------------------------------------------------------------------

###############################
## OPTION 1: For all Tuna-RFMOs
###############################
CREATE TEMP FUNCTION res_factor() AS (5);
CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2021-01-01");
CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2021-12-31");
CREATE TEMP FUNCTION target_rfmos() AS (["CCSBT", "IATTC", "ICCAT", "IOTC", "WCPFC"]);
CREATE TEMP FUNCTION target_vessel_class() AS (["drifting_longlines", "tuna_purse_seines", "pole_and_line", "trollers"]);

########################################
## OPTION 2: For all squid fishing areas
########################################
-- CREATE TEMP FUNCTION res_factor() AS (5);
-- CREATE TEMP FUNCTION start_date() AS (TIMESTAMP "2021-01-01");
-- CREATE TEMP FUNCTION end_date() AS (TIMESTAMP "2021-12-31");
-- CREATE TEMP FUNCTION target_rfmos() AS (["NPFC", "SPRFMO"]);
-- CREATE TEMP FUNCTION target_vessel_class() AS (["squid_jigger"]);
----------------------------------------------------------------------


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
      #############################################################
      ## If you want to filter only target classes for OPTION 1,2,4
      ## like tuna fishing vessels for tuna RFMO
      ## For Option 3, comment it out so that you can see squid
      ## fishing in Indian Ocean and off Argentinian waters where
      ## no RFMO is regulating squid fisheries
      #############################################################
      AND best_vessel_class IN UNNEST (target_vessel_class())
  ),

  ---------------------------------------------
  -- Pull fishing vessel identity data
  -- that is active within the given time range
  ---------------------------------------------
  on_identity_db AS (
    SELECT DISTINCT ssvid, first_timestamp, last_timestamp
    FROM identity_core_data
    WHERE is_fishing
      AND (first_timestamp <= end_date()
        AND last_timestamp >= start_date())
  ),

  --------------------------------
  -- Identity known vessels on AIS
  --------------------------------
  known_vessels AS (
    SELECT DISTINCT ssvid, first_timestamp, last_timestamp, best_flag, best_vessel_class
    FROM on_ais AS a
    JOIN on_identity_db AS b
    USING (ssvid)
  ),

  -----------------------------------------------------------------
  -- Attach authorization information to the identity known vessels
  -----------------------------------------------------------------
  authorized_vessels AS (
    SELECT DISTINCT
      ssvid, best_flag, best_vessel_class, first_timestamp, last_timestamp,
      source_code, authorized_from, authorized_to
    FROM known_vessels AS a
    JOIN identity_authorization_data AS b
    USING (ssvid)
    WHERE b.authorized_from <= last_timestamp
      AND b.authorized_to >= first_timestamp
      AND source_code IN UNNEST (target_rfmos())
  ),

  ---------------------------------------------------------------
  -- RFMO/EEZ information per (lat, lon) cell in 100th resolution
  ---------------------------------------------------------------
  region_info AS (
    SELECT gridcode, region, fao_area
    FROM `world-fishing-827.vessel_identity_staging.regions`
  ),

  ---------------------
  -- Raw fishing effort
  ---------------------
  fishing_effort_raw AS (
    SELECT * EXCEPT (region, fao_area), region AS rfmo
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
          WHERE timestamp BETWEEN start_date() AND end_date()
            AND seg_id IN (
              SELECT seg_id
              FROM `gfw_research.pipe_v20201001_segs`
              WHERE good_seg
                AND NOT overlapping_and_short ) )
        GROUP BY 1,2,3,4 )
      WHERE date BETWEEN DATE (start_date()) AND DATE (end_date()) )
    INNER JOIN region_info
    USING (gridcode)
    ----------------------------
    -- Filter only the high seas
    ----------------------------
    WHERE EXISTS (
      SELECT *
      FROM UNNEST (region) AS r
      WHERE r = "highseas" )
  ),

  -----------------------------------------------------------------------
  -- Filter fishing effort for those vessels in AIS and areas of interest
  -----------------------------------------------------------------------
  fishing_effort AS (
    SELECT
      ssvid, lat, lon, rfmo, date, best_flag, best_vessel_class, fishing_hours
    FROM fishing_effort_raw
    JOIN on_ais
    USING (ssvid)
    WHERE year = EXTRACT (YEAR FROM date)
      ----------------------------------------------------------------
      -- For IATTC and ICCAT, authorization data start from 2019-05-01
      ----------------------------------------------------------------
      AND NOT ( date < "2019-05-01"
        AND EXISTS (SELECT * FROM UNNEST (rfmo) AS r WHERE r IN ("IATTC", "ICCAT") ) )
      ##################################################################################
      ## To limit areas of interest, for instance some squid fishing
      ## grounds are covered by no RFMO regions therefore remove this block for OPTION 2
      ##################################################################################
      AND EXISTS (
        SELECT *
        FROM UNNEST (rfmo) AS r
        WHERE r IN UNNEST (target_rfmos()) )
  ),

  ------------------------------------------------------------
  -- Make binned fishing effort rasters for all vessels in AIS
  ------------------------------------------------------------
  fishing_effort_binned_all_vessels AS (
    SELECT
      best_flag,
      FLOOR (lat * res_factor()) AS lat_bin,
      FLOOR (lon * res_factor()) AS lon_bin,
      SUM (fishing_hours) AS fishing_hours
    FROM (
      ----------------------------------------------
      -- DISTINCT is important because the same cell
      -- may be duplicated due to RFMO overlaps
      ----------------------------------------------
      SELECT DISTINCT best_flag, lat, lon, fishing_hours, date
      FROM fishing_effort )
    GROUP BY 1,2,3
  ),

  ----------------------------------------------------------
  -- Fishing effort for the authorization identified vessels
  ----------------------------------------------------------
  fishing_effort_authorized_vessels AS (
    SELECT DISTINCT
      a.ssvid, a.best_flag, a.best_vessel_class,
      lat, lon, date, rfmo, fishing_hours
    FROM fishing_effort AS a
    LEFT JOIN UNNEST (rfmo) AS rfmo
    JOIN authorized_vessels AS b
    ON a.ssvid = b.ssvid
      AND a.date BETWEEN DATE (b.authorized_from) AND DATE (b.authorized_to)
      AND source_code = rfmo
  ),

  ------------------------------------------------------------
  -- Make binned fishing effort rasters for authorized vessels
  ------------------------------------------------------------
  fishing_effort_binned_authorized_vessels AS (
    SELECT
      best_flag,
      FLOOR (lat * res_factor()) AS lat_bin,
      FLOOR (lon * res_factor()) AS lon_bin,
      SUM (fishing_hours) AS fishing_hours
    FROM (
      ----------------------------------------------
      -- DISTINCT is important because the same cell
      -- may be duplicated due to RFMO overlaps
      ----------------------------------------------
      SELECT DISTINCT best_flag, lat, lon, fishing_hours, date
      FROM fishing_effort_authorized_vessels )
    GROUP BY 1,2,3
  ),

  --------------------------------------------------------------
  -- Make binned fishing effort raster for authorization unknown
  --------------------------------------------------------------
  fishing_effort_binned_auth_unknown_vessels AS (
    SELECT DISTINCT
      best_flag, lat_bin, lon_bin,
      a.fishing_hours - IFNULL (b.fishing_hours, 0) AS fishing_hours
    FROM fishing_effort_binned_all_vessels AS a
    LEFT JOIN fishing_effort_binned_authorized_vessels AS b
    USING (best_flag, lat_bin, lon_bin)
  ),

  -------------------------------------------------------------
  -- Authorization known vs. unknown fishing ratio by rfmo
  -- to compare authorization identified ratio among Tuna RFMOs
  -------------------------------------------------------------
  auth_unknown_fishing_by_rfmo AS (
    SELECT
      rfmo,
      ROUND (SUM (fishing_hours_auth_unknown), 0) AS fishing_hours_auth_unknown,
      ROUND (SUM (fishing_hours_total), 0) AS fishing_hours_total,
      ROUND (SUM (fishing_hours_auth_unknown) / SUM (fishing_hours_total), 2) AS ratio_auth_unknown_fishing
    FROM (
      SELECT rfmo, fishing_hours_total, fishing_hours_auth_unknown
      FROM (
        SELECT
          assign_gridcode (lon, lat) AS gridcode,
          a.fishing_hours AS fishing_hours_total,
          a.fishing_hours - IFNULL (b.fishing_hours, 0) AS fishing_hours_auth_unknown
        FROM (
          SELECT lat, lon, SUM (fishing_hours) AS fishing_hours
          FROM (
            SELECT DISTINCT lat, lon, date, fishing_hours
            FROM fishing_effort)
          GROUP BY 1,2) AS a
        LEFT JOIN (
          SELECT lat, lon, SUM (fishing_hours) AS fishing_hours
          FROM (
            SELECT DISTINCT lat, lon, date, fishing_hours
            FROM fishing_effort_authorized_vessels )
          GROUP BY 1,2) AS b
        USING (lat, lon) )
      INNER JOIN (
        SELECT DISTINCT gridcode, rfmo
        FROM region_info
        LEFT JOIN UNNEST(region) AS rfmo )
      USING (gridcode)
      WHERE rfmo IN UNNEST (target_rfmos() ) )
    GROUP BY 1
    HAVING fishing_hours_total > 0
    ORDER BY rfmo, ratio_auth_unknown_fishing DESC
  ),

  ------------------------------------------------------
  -- Create final auth vs all fishing raster for mapping
  ------------------------------------------------------
  authorized_fishing_gridded AS (
    SELECT
      lat_bin, lon_bin,
      fishing_hours AS fishing_hours_all,
      IFNULL (fishing_hours_authorized_vessels, 0) AS fishing_hours_authorized_vessels
    FROM (
      SELECT lat_bin, lon_bin, SUM (fishing_hours) AS fishing_hours
      FROM fishing_effort_binned_all_vessels
      GROUP BY 1,2 )
    LEFT JOIN (
      SELECT lat_bin, lon_bin, SUM (fishing_hours) AS fishing_hours_authorized_vessels
      FROM fishing_effort_binned_authorized_vessels
      GROUP BY 1,2 )
    USING (lat_bin, lon_bin)
    WHERE fishing_hours > 0
  )

#########################################################################
-------------------------------------------------------------------------
-- OPTIONS FOR OUTPUT: You may want to choose one final query block below
-- to produce output in which you are interested
-------------------------------------------------------------------------


##########################################################
## OPTION 1: Compare authorization unknown fishing by RFMO
##########################################################
-- SELECT * FROM auth_unknown_fishing_by_rfmo ORDER BY rfmo

##########################################
## OPTION 2: Fishing effort gridded raster
##########################################
SELECT * FROM authorized_fishing_gridded
