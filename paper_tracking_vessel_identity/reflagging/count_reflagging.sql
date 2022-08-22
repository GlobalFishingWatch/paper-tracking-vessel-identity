--###############
--# Top 20% count
--###############
WITH
  --------------------------
  -- Latest reflagging table
  --------------------------
  raw_data AS (
    SELECT *
    FROM `vessel_identity.reflagging_core_all_v20220701`
  ),

  ----------------------------------------------------
  -- Add the immediate next flag that a vessel reflags
  ----------------------------------------------------
  add_next_flag AS (
    SELECT *, LEAD (flag_eu) OVER (PARTITION BY vessel_record_id ORDER BY first_timestamp, last_timestamp) AS next_flag_eu
    FROM raw_data
  ),

  ---------------------------------------------------------------
  -- Exclude if a vessel changes identity but keeps the same flag
  -- and keep the final identity of a vessel hull
  ---------------------------------------------------------------
  reflagging_instances AS (
    SELECT vessel_record_id, flag_eu, next_flag_eu
    FROM add_next_flag
    WHERE (flag_eu != next_flag_eu
      OR next_flag_eu IS NULL)
  ),

  ---------------------------------------------------
  -- Count the number of reflagging instances by flag
  ---------------------------------------------------
  count_by_flag AS (
    SELECT flag_eu, COUNT (*) AS cnt
    FROM reflagging_instances
    GROUP BY 1 ORDER BY cnt DESC
  ),

  -----------------------------
  -- Total reflagging instances
  -----------------------------
  cnt_all AS (
    SELECT SUM (cnt) AS total_cnt
    FROM count_by_flag
    WHERE flag_eu != "UNK"
  ),

  --------------------------------------------
  -- Reflagging instances by the top 20% flags
  -- 26 flags out of 116 flags in total
  --------------------------------------------
  cnt_top20percent AS (
    SELECT SUM (cnt) AS top20_cnt
    FROM (
      SELECT cnt, flag_eu, RANK () OVER (ORDER BY cnt DESC) AS rank_flag
      FROM count_by_flag )
    WHERE rank_flag <= 26
  )

SELECT top20_cnt, total_cnt, top20_cnt / total_cnt AS ratio
FROM cnt_top20percent
LEFT JOIN cnt_all
ON TRUE

--######################
--# All identity changes
--######################
-- SELECT DISTINCT vessel_record_id
-- FROM (
-- SELECT *, 
--   COUNT (DISTINCT n_shipname) OVER (PARTITION BY vessel_record_id ) AS cnt1, 
--   COUNT (DISTINCT n_callsign) OVER (PARTITION BY vessel_record_id ) AS cnt2, 
--   COUNT (DISTINCT flag) OVER (PARTITION BY vessel_record_id ) AS cnt3, 
--   COUNT (DISTINCT ssvid) OVER (PARTITION BY vessel_record_id ) AS cnt4, 
-- FROM `vessel_identity.identity_core_v20220701` )
-- WHERE cnt1 > 1 OR cnt2> 1 OR cnt3 > 1 #OR cnt4 > 1

--#####################################
--# Reflagging ratio of fishing vessels
--#####################################
-- SELECT 
--   cnt_reflag_fishing, 
--   cnt_total_fishing, 
--   cnt_reflag_fishing / cnt_total_fishing AS ratio
-- FROM (
--   SELECT COUNT (DISTINCT (vessel_record_id)) AS cnt_reflag_fishing 
--   FROM `vessel_identity.reflagging_core_fishing_v20220701` ),
--   (SELECT COUNT (DISTINCT (vessel_record_id)) AS cnt_total_fishing
--    FROM `vessel_identity.identity_core_v20220701`
--    WHERE is_fishing)

--#####################################
--# Reflagging ratio of support vessels
--#####################################
-- SELECT 
--   cnt_reflag_support, 
--   cnt_total_support, 
--   cnt_reflag_support / cnt_total_support AS ratio
-- FROM (
--   SELECT COUNT (DISTINCT (vessel_record_id)) AS cnt_reflag_support
--   FROM `vessel_identity.reflagging_core_support_v20220701` ),
--   (SELECT COUNT (DISTINCT (vessel_record_id)) AS cnt_total_support
--    FROM `vessel_identity.identity_core_v20220701`
--    WHERE is_carrier OR is_bunker)
