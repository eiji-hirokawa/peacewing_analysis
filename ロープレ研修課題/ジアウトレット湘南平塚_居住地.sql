-- テーブルの作成
CREATE OR REPLACE TABLE `prd-analysis.w_e_hirokawa.role_playing_shonan_hashed_adid_poi_home` AS

SELECT
  count(distinct hashed_adid) as hashed_adid_cnt,
  weekday_flag,
  holiday_flag,
  date(sdk_detect_ptime) as date,
  poi_home_mesh,
  poi_home_mesh_lat,
  poi_home_mesh_lon
FROM
  `prd-analysis.w_e_hirokawa.role_playing_shonan`
where
  poi_home_mesh_lat is not null and
  poi_home_mesh_lon is not null
group by poi_home_mesh, weekday_flag, holiday_flag, date(sdk_detect_ptime), poi_home_mesh_lat, poi_home_mesh_lon