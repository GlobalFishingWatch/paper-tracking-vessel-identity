WITH
  --------------------------
  -- Latest reflagging table
  --------------------------
  raw_data AS (
    SELECT *
    FROM `vessel_identity.reflagging_core_all_v20211101`
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
  -- 25 flags out of 115 flags in total
  --------------------------------------------
  cnt_top20percent AS (
    SELECT SUM (cnt) AS top20_cnt
    FROM (
      SELECT cnt, flag_eu, RANK () OVER (ORDER BY cnt DESC) AS rank_flag
      FROM count_by_flag )
    WHERE rank_flag <= 25
  )

SELECT top20_cnt, total_cnt, top20_cnt / total_cnt AS ratio
FROM cnt_top20percent
LEFT JOIN cnt_all
ON TRUE
