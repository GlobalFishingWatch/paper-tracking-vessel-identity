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
    FROM `vessel_identity_staging.identity_stitcher_core_filtered_v20220701`
  ),

  reflagging AS (
    SELECT
      port_iso3, port_label, flag, pair_flag,
    FROM raw_data
    WHERE port_label = "BUSAN"
  ),

  flag_rank AS (
    SELECT flag, cnt, RANK () OVER (ORDER BY cnt DESC, flag DESC) AS rank
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
png("/Users/jaeyoon/gfw/vessel-identity/database-publication-Q1-2021/identity_stitcher/reflagging_busan.png",
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
  RUS="#ee6256", KOR="#f8ba47", PAN="#8abbc7", KHM="#f1816f", SLE="#ad2176",
  LVA="#00ffc3", VUT="#c77ca1", LBR="#fbd38f", MHL="#fde0b1", JPN="#f59e8b",
  TUV="#363c4c", TGO="#093b76", KIR="#b9558a", BLZ="#848b9b", MLT="#e6e7eb",
  OTHERS="#b2b2b2"
)
circos.par(gap.after = 4)
chordDiagram(as.matrix(final),
             grid.col = grid.col,
             order = c("RUS", 'KOR', 'PAN', 'KHM', 'SLE', 
                       'TGO', 'TUV', 'LVA', 'VUT', 'LBR', 
                       'KIR', 'BLZ', 'DMA', 'JPN', 'MHL', 'OTHERS'),
             directional = -1, direction.type = c("diffHeight", "arrows"),
             link.arr.type = "big.arrow", diffHeight = -mm_h(1))

# circos.track(track.index = 1, panel.fun = function(x, y) {
#   circos.text(CELL_META$xcenter, CELL_META$ylim[1], CELL_META$sector.index,
#               facing = "clockwise", niceFacing = TRUE, adj = c(0, 0.5))
# }, bg.border = NA) # here set bg.border to NA is important

title("Vessel Flag Changes in Busan, Rep. of Korea", cex.main=1)

dev.off()
