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
    WHERE port_label = "ZHOUSHAN" #"BUSAN" #"LAS PALMAS" #KAOHSIUNG"
  ),
  
  flag_rank AS (
    SELECT flag, cnt, RANK () OVER (ORDER BY cnt DESC) AS rank
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
  IF (b.rank <= 15, a.flag, "OTHERS") AS flag,
  IF (c.rank <= 15, a.pair_flag, "OTHERS") AS pair_flag,
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
png("/Users/jaeyoon/gfw/vessel-identity/database-publication-Q1-2021/identity_stitcher/reflagging_zhoushan.png", 
    width=2400, height = 2400, res=450)

## all of your plotting code
flag <- df$data %>% pull(flag)
pair_flag <- df$data %>% pull(pair_flag)
cnt <- data.matrix(df$data %>% pull(cnt))
sp <- spread(df$data, flag, cnt)
cflag = sp[,1]$pair_flag
final <- sp[,!colnames(sp) %in% "pair_flag"]
rownames(final) <- cflag

grid.col = c(
  CHN="#d73b68", PAN="#204280", KIR="#f8ba47", KHM="#8abbc7", LBR="#f68d4b",
  BLZ="#00ffc3", BHS="#c77ca1", SLE="#fbd38f", PLW="#fde0b1", TGO="#f59e8b",
  BRB="#363c4c", VCT="#093b76", SGP="#b9558a", MLT="#848b9b", DMA="#d73b68", 
  KOR="#b2b2b2"
) 
circos.par(gap.after = 4)
chordDiagram(as.matrix(final), 
             order = c('PAN', 'CHN', 'KIR', 'KHM', 'LBR',
                       'BLZ', 'BHS', 'SLE', 'PLW', 'TGO',
                       'BRB', 'VCT', 'DMA', 'KOR', 'SGP', 'MLT'),
             grid.col = grid.col,
             directional = -1, direction.type = c("diffHeight", "arrows"),
             link.arr.type = "big.arrow", diffHeight = -mm_h(1))

# circos.track(track.index = 1, panel.fun = function(x, y) {
#   circos.text(CELL_META$xcenter, CELL_META$ylim[1], CELL_META$sector.index, 
#               facing = "clockwise", niceFacing = TRUE, adj = c(0, 0.5))
# }, bg.border = NA) # here set bg.border to NA is important

title("Vessel Flag Changes in Zhoushan, China", cex.main=1)

dev.off()
