declare polygon_the_outlet_shonanhiratsuka, polygon_mitsui_shopping_park_shonanhiratsuka geography;
declare mesh_the_outlet_shonanhiratsuka, mesh_mitsui_shopping_park_shonanhiratsuka array<int64>;

-- ポリゴンの指定
set polygon_the_outlet_shonanhiratsuka = ST_GeogFromText('POLYGON((139.36174518323728 35.39326583710515,139.36173445440122 35.38882269444102,139.36292535520383 35.38897138624475,139.3643415615637 35.39113175976157,139.3672061607916 35.39122796938196,139.36716324544736 35.393318313705464,139.36174518323728 35.39326583710515))');
set polygon_mitsui_shopping_park_shonanhiratsuka = ST_GeogFromText('POLYGON((139.3535387571236 35.33798160217742,139.35365140990223 35.33669503402216,139.35276091650928 35.336471851747184,139.3530881460091 35.33549159288763,139.35583472804035 35.33583731053913,139.3575352485558 35.33641933818113,139.35715437487568 35.337574628751376,139.3550139720818 35.337150148908194,139.35482085303272 35.33802973868365,139.3535387571236 35.33798160217742))');

-- メッシュの指定
set mesh_the_outlet_shonanhiratsuka = `prd-analysis.master_candidate.BW_CONVERT_POLYGON_TO_MESH`(polygon_the_outlet_shonanhiratsuka);
set mesh_mitsui_shopping_park_shonanhiratsuka = `prd-analysis.master_candidate.BW_CONVERT_POLYGON_TO_MESH`(polygon_mitsui_shopping_park_shonanhiratsuka);

-- テーブルの作成
CREATE OR REPLACE TABLE `prd-analysis.w_e_hirokawa.role_playing_shonan_hashed_adid_count_monthly` AS

with tmp as (
  select
    hashed_adid,
    sdk_detect_ptime,
    case
      when ST_COVERS(polygon_the_outlet_shonanhiratsuka, st_geogpoint(longitude, latitude)) then 1
      else 0
    end as visit_the_outlet_shonanhiratsuka,
    case
      when ST_COVERS(polygon_mitsui_shopping_park_shonanhiratsuka, st_geogpoint(longitude, latitude)) then 1
      else 0
    end as visit_mitsui_shopping_park_shonanhiratsuka,
  from
    `prd-analysis.master_v.sdk_master_table_bq`
  where
    -- 計算を軽くするために日付とメッシュで絞る
    date(sdk_detect_ptime, 'Asia/Tokyo') between "2024-01-01" and "2024-04-30" and
    (
      cast(mesh as int64) in unnest(mesh_the_outlet_shonanhiratsuka) or
      cast(mesh as int64) in unnest(mesh_mitsui_shopping_park_shonanhiratsuka)
    ) and

    -- 時間と場所を指定する
    (
        ST_COVERS(polygon_the_outlet_shonanhiratsuka, st_geogpoint(longitude, latitude)) or
        ST_COVERS(polygon_mitsui_shopping_park_shonanhiratsuka, st_geogpoint(longitude, latitude))
    ) and

    -- 不正なhashed_adidの除外
    hashed_adid not in (
      '9f89c84a559f573636a47ff8daed0d33', #00000000-0000-0000-0000-000000000000
      'd41d8cd98f00b204e9800998ecf8427e', #空白
      '37a6259cc0c1dae299a7866489dff0bd', #文字列null
      '81684c2e68ade2cd4bf9f2e8a67dd4fe'  #ALLnull
    ) and
    hashed_adid is not null
)

SELECT
  FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP(sdk_detect_ptime), 'Asia/Tokyo') AS month,
  COUNT(DISTINCT CASE WHEN visit_the_outlet_shonanhiratsuka = 1 THEN hashed_adid END) AS visit_count_the_outlet_shonanhiratsuka,
  COUNT(DISTINCT CASE WHEN visit_mitsui_shopping_park_shonanhiratsuka = 1 THEN hashed_adid END) AS visit_count_mitsui_shopping_park_shonanhiratsuka
FROM
  tmp
GROUP BY
  month
ORDER BY
  month