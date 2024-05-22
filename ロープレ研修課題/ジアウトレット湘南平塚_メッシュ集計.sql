-- テーブルの作成
CREATE OR REPLACE TABLE `prd-analysis.w_e_hirokawa.role_playing_shonan_hashed_adid_count_in_mesh` AS

SELECT
  count(distinct hashed_adid) as hashed_adid_cnt,
  weekday_flag,
  holiday_flag,
  date(sdk_detect_ptime) as date,
  mesh,
  visitor_mesh_lat,
  visitor_mesh_lon
FROM
  `prd-analysis.w_e_hirokawa.role_playing_shonan`
group by mesh, weekday_flag, holiday_flag, date(sdk_detect_ptime), visitor_mesh_lat, visitor_mesh_lon