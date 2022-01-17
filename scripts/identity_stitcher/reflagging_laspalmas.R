# Load fishwatchr
library(fishwatchr)

# Other useful packages
library(tidyverse)
library(glue)
# library(geom_alluvium)
library(alluvial)
library(circlize)

# Establish connection to BigQuery project
con <- DBI::dbConnect(drv = bigrquery::bigquery(), project = "world-fishing-827", use_legacy_sql = FALSE)

sql <- c('
  WITH 
  raw_data AS (
    SELECT *
    FROM `vessel_identity_staging.identity_stitcher_core_v20210801`
  ),
  
  reflagging AS (
    SELECT 
      port_iso3, port_label, flag, pair_flag, 
    FROM (
      SELECT 
        imo, pair_imo, ssvid, pair_ssvid, flag, pair_flag, port_iso3, 
        IF (port_label = "KAOSIUNG", "KAOHSIUNG", port_label) AS port_label, 
        first_timestamp, last_timestamp, n_shipname, pair_n_shipname, distance_gap_meter, time_gap_minute,
      FROM raw_data
      WHERE (imo = pair_imo OR vessel_record_id = pair_vessel_record_id) 
        OR ((imo IS NULL OR pair_imo IS NULL) AND distance_gap_meter <= 30) ) 
    WHERE port_label = "LAS PALMAS" #"BUSAN" #"ZHOUSHAN" #KAOHSIUNG"
  ),
  
  flag_rank AS (
    SELECT flag, cnt, RANK () OVER (ORDER BY cnt DESC, flag) AS rank
    FROM (
      SELECT flag, COUNT (*) AS cnt
      FROM (
        SELECT flag
        FROM reflagging
        UNION ALL
        SELECT pair_flag AS flag
        FROM reflagging )
      GROUP BY 1 )
  )
  
SELECT 
  IF (b.rank <= 20, a.flag, "OTHERS") AS flag,
  IF (c.rank <= 20, a.pair_flag, "OTHERS") AS pair_flag,
  COUNT (*) AS cnt
FROM reflagging AS a
LEFT JOIN flag_rank AS b
USING (flag)
LEFT JOIN flag_rank AS c
ON (a.pair_flag = c.flag)
GROUP BY 1,2

')

df <- gfw_query(query = sql, 
                run_query = TRUE, 
                con = con)

circos.clear()
png("/Users/jaeyoon/gfw/vessel-identity/database-publication-Q1-2021/identity_stitcher/reflagging_laspalmas.png", 
    width=2400, height=2400, res=450)

## all of your plotting code
flag <- df$data %>% pull(flag)
pair_flag <- df$data %>% pull(pair_flag)
cnt <- data.matrix(df$data %>% pull(cnt))
sp <- spread(df$data, flag, cnt)
cflag = sp[,1]$pair_flag
final <- sp[,!colnames(sp) %in% "pair_flag"]
rownames(final) <- cflag

grid.col = c(
  PAN="#204280", BLZ="#ad2176", KNA="#ee6256", RUS="#f8ba47", COM="#8abbc7",
  GEO="#e74327", CUW="#88d498", CYP="#7277a4", JPN="#f9c66d", LTU="#9d74a6",
  LVA="#363c4c", MAR="#093b76", GIN="#3b9088", DNK="#848b9b", MRT="#4b5b91", 
  POL="#4b5b91", LBR="#00ffc3", NOR="#7277a4", CMR="#f9c66d", GNB="#d73b68", 
  OTHERS="#b2b2b2"
  # 
) 
circos.par(gap.after = 4)
chordDiagram(as.matrix(final), 
             order = c('BLZ', 'PAN', 'KNA', 'RUS', 'COM',
                       'GEO', 'MRT', 'JPN', 'LTU', 'CMR', 
                       'CYP', 'GIN', 'POL', 'LBR', 'MAR',   
                       'NOR', 'CUW', 'LVA', 'DNK', 'GNB', 'OTHERS'),
             grid.col = grid.col,
             directional = -1, direction.type = c("diffHeight", "arrows"),
             link.arr.type = "big.arrow", diffHeight = -mm_h(1))

# circos.track(track.index = 1, panel.fun = function(x, y) {
#   circos.text(CELL_META$xcenter, CELL_META$ylim[1], CELL_META$sector.index, 
#               facing = "clockwise", niceFacing = TRUE, adj = c(0, 0.5))
# }, bg.border = NA) # here set bg.border to NA is important

title("Vessel Flag Changes in Las Palmas, Spain", cex.main=1)

dev.off()
