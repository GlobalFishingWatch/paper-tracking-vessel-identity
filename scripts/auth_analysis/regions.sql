------------------------------------------------------------------------
-- This query generates a table containing information about region
-- (RFMO, EEZ, FAO areas etc) that is much smaller than the original
-- table. The result table is intended for the use of the identity paper
------------------------------------------------------------------------

------------------------------------------------------------------------------
-- Active the following line commented out only to create or replace the table
------------------------------------------------------------------------------
-- CREATE OR REPLACE TABLE vessel_identity_staging.regions
--   (gridcode STRING, region ARRAY<STRING>, fao_area STRING) AS
------------------------------------------------------------------------------

WITH
  raw_data AS (
    SELECT
      gridcode,
      IF (ARRAY_LENGTH (regions.eez) = 0, ["highseas"], regions.eez) AS eez,
      regions.rfmo, regions.fao, regions.ocean
    FROM `pipe_static.regions`
  ),

  filter_eez_rfmo AS (
    SELECT
      gridcode,
      ARRAY (
        SELECT iso3 AS eez
        FROM UNNEST (eez) AS eez
        JOIN (
          SELECT CAST (eez_id AS STRING) AS eez_id, territory1_iso3 AS iso3
          FROM `gfw_research.eez_info`
          WHERE eez_type = "200NM"
          UNION ALL
          SELECT "highseas" AS eez_id, "highseas" AS iso3)
        ON (eez = eez_id)
        WHERE iso3 IN ("highseas", "PER", "FRO", "NOR", "ISL") ) AS eez,
      ARRAY (
        SELECT rfmo
        FROM UNNEST (rfmo) AS rfmo
        WHERE rfmo IN (
          "CCSBT", "IATTC", "ICCAT", "IOTC", "WCPFC",
          "NPFC", "SPRFMO", "CCAMLR", "GFCM", "SIOFA",
          "NEAFC", "NAFO", "SEAFO", "FFA" ) ) AS rfmo,
      ARRAY (
        SELECT "NEAFC"
        FROM UNNEST (fao) AS fao
        WHERE fao IN (
          "27.1.a", "27.2.a", "27.2.b", "27.5.b", "27.6.b",
          "27.7.c", "27.7.k", "27.7.j", "27.8.d", "27.8.e",
          "27.9.b", "27.10.a", "27.10.b", "27.12.a", "27.12.b",
          "27.12.c", "27.14.b") ) AS neafc,
      ARRAY (
        SELECT "SEAFO"
        FROM UNNEST (fao) AS fao
        WHERE fao IN (
          "47.A.0", "47.A.1", "47.B.0", "47.B.1",
          "47.C.0", "47.C.1", "47.D.0", "47.D.1", "34.4.1") ) AS seafo,
      ARRAY (
        SELECT ocean
        FROM UNNEST (ocean) AS ocean ) AS ocean
    FROM raw_data
  ),

  fao_region AS (
    SELECT gridcode,
      ARRAY (
        SELECT fao
        FROM UNNEST (fao) AS fao
        WHERE fao IN (
          "18", "21", "27", "31", "34",
          "37", "41", "47", "48", "51",
          "57", "58", "61", "67", "71",
          "77", "81", "87", "88") ) AS fao
    FROM raw_data
  ),

  concat_eez_rfmo AS (
    SELECT gridcode,
      ARRAY (
        SELECT DISTINCT *
        FROM UNNEST (ARRAY_CONCAT (
          eez, rfmo, neafc, seafo, ocean))) AS region,
      fao[SAFE_OFFSET(0)] AS fao_area
    FROM filter_eez_rfmo
    LEFT JOIN fao_region
    USING (gridcode)
  )

SELECT gridcode, region, fao_area
FROM concat_eez_rfmo
