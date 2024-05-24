declare polygon_mitsui_shopping_park_shonanhiratsuka geography;
declare mesh_mitsui_shopping_park_shonanhiratsuka array<int64>;

-- ポリゴンの指定
set polygon_mitsui_shopping_park_shonanhiratsuka = ST_GeogFromText('POLYGON((139.3535387571236 35.33798160217742,139.35365140990223 35.33669503402216,139.35276091650928 35.336471851747184,139.3530881460091 35.33549159288763,139.35583472804035 35.33583731053913,139.3575352485558 35.33641933818113,139.35715437487568 35.337574628751376,139.3550139720818 35.337150148908194,139.35482085303272 35.33802973868365,139.3535387571236 35.33798160217742))');

-- メッシュの指定
set mesh_mitsui_shopping_park_shonanhiratsuka = `prd-analysis.master_candidate.BW_CONVERT_POLYGON_TO_MESH`(polygon_mitsui_shopping_park_shonanhiratsuka);

-- テーブルの作成
CREATE OR REPLACE TABLE `prd-analysis.w_e_hirokawa.role_playing_shonan_mitsui_hased_adid_poi_home` AS

-- 三井ショッピングパーク湘南平塚の来訪者のhashed_adidを取得
with visitor_mitsui_shopping_park_shonanhiratsuka_weekday as (
  select distinct
    hashed_adid,
    1 as weekday_flag
  from
    `prd-analysis.master_v.sdk_master_table_bq`
  where
    -- 計算を軽くするために日付とメッシュで絞る
    date(sdk_detect_ptime, 'Asia/Tokyo') between "2024-04-15" and "2024-04-19" and
    cast(mesh as int64) in unnest(mesh_mitsui_shopping_park_shonanhiratsuka) and

    -- 時間と場所を指定する
    ST_COVERS(polygon_mitsui_shopping_park_shonanhiratsuka, st_geogpoint(longitude, latitude)) and

    -- 不正なhashed_adidの除外
    hashed_adid not in (
      '9f89c84a559f573636a47ff8daed0d33', #00000000-0000-0000-0000-000000000000
      'd41d8cd98f00b204e9800998ecf8427e', #空白
      '37a6259cc0c1dae299a7866489dff0bd', #文字列null
      '81684c2e68ade2cd4bf9f2e8a67dd4fe'  #ALLnull
    ) and
    hashed_adid is not null
),

-- 三井ショッピングパーク湘南平塚の来訪者のhashed_adidを取得
visitor_mitsui_shopping_park_shonanhiratsuka_holiday as (
  select distinct
    hashed_adid,
    1 as holiday_flag
  from
    `prd-analysis.master_v.sdk_master_table_bq`
  where
    -- 計算を軽くするために日付とメッシュで絞る
    (
      date(sdk_detect_ptime, 'Asia/Tokyo') = "2024-04-13" or
      date(sdk_detect_ptime, 'Asia/Tokyo') = "2024-04-14" or
      date(sdk_detect_ptime, 'Asia/Tokyo') = "2024-04-20" or
      date(sdk_detect_ptime, 'Asia/Tokyo') = "2024-04-21") and
    cast(mesh as int64) in unnest(mesh_mitsui_shopping_park_shonanhiratsuka) and

    -- 時間と場所を指定する
    ST_COVERS(polygon_mitsui_shopping_park_shonanhiratsuka, st_geogpoint(longitude, latitude)) and

    -- 不正なhashed_adidの除外
    hashed_adid not in (
      '9f89c84a559f573636a47ff8daed0d33', #00000000-0000-0000-0000-000000000000
      'd41d8cd98f00b204e9800998ecf8427e', #空白
      '37a6259cc0c1dae299a7866489dff0bd', #文字列null
      '81684c2e68ade2cd4bf9f2e8a67dd4fe'  #ALLnull
    ) and
    hashed_adid is not null
),

visitor_hashed_adid as (
  select distinct
    hashed_adid
  from
    visitor_mitsui_shopping_park_shonanhiratsuka_weekday
  union all
  select distinct
    hashed_adid
  from
    visitor_mitsui_shopping_park_shonanhiratsuka_holiday
),

visitor_mitsui_shopping_park_shonanhiratsuka as(
  select distinct
    a.hashed_adid,
    b.weekday_flag,
    c.holiday_flag
  from
    visitor_hashed_adid a
  left join
    visitor_mitsui_shopping_park_shonanhiratsuka_weekday b
    on
      a.hashed_adid = b.hashed_adid
  left join
    visitor_mitsui_shopping_park_shonanhiratsuka_holiday c
    on
      a.hashed_adid = c.hashed_adid
),

-- メッシュのマスターテーブルに、複数の長さのメッシュIDが含まれていた
-- 重複を避けるために、長さが8のメッシュのみを使用する
-- 正規のやり方がわからないのであとで確認したい
mesh as (
  select
    mesh,
    max(upper_lat) as upper_lat,
    min(lower_lat) as lower_lat,
    max(upper_lon) as upper_lon,
    min(lower_lon) as lower_lon
  from
    `prd-analysis.data_dev_team.mesh_table`
  where
    length(cast(mesh as string)) = 8
  group by mesh
)

-- 三井ショッピングパーク湘南平塚の来訪者の行動ログを取得
select
  a.hashed_adid,
  a.weekday_flag,
  a.holiday_flag,
  b.sdk_detect_ptime,
  b.latitude,
  b.longitude,
  b.accuracy,
  b.mesh,
  c.poi_home_mesh,
  (cast(d.upper_lat as float64) + cast(d.lower_lat as float64)) / 2 as visitor_mesh_lat,
  (cast(d.upper_lon as float64) + cast(d.lower_lon as float64)) / 2 as visitor_mesh_lon,
  (cast(e.upper_lat as float64) + cast(e.lower_lat as float64)) / 2 as poi_home_mesh_lat,
  (cast(e.upper_lon as float64) + cast(e.lower_lon as float64)) / 2 as poi_home_mesh_lon
from
  visitor_mitsui_shopping_park_shonanhiratsuka a
left join
  `prd-analysis.master_v.sdk_master_table_bq` b
  on a.hashed_adid = b.hashed_adid
left join
  `master_v.poi_li_monthly_hashed_adid` c
  on
    a.hashed_adid = c.hashed_adid and
    FORMAT_TIMESTAMP('%Y-%m', date(b.sdk_detect_ptime, 'Asia/Tokyo')) = FORMAT_TIMESTAMP('%Y-%m', c.date)
left join
  mesh d
  on
    left(cast(b.mesh as string), 8) = cast(d.mesh as string) -- 右のメッシュの長さが8なので、左も8桁分取得する。正規のやり方要確認
left join
  mesh e
  on
    left(cast(c.poi_home_mesh as string), 8) = cast(e.mesh as string) -- 右のメッシュの長さが8なので、左も8桁分取得する。正規のやり方要確認
where
  date(b.sdk_detect_ptime, 'Asia/Tokyo') between "2024-04-13" and "2024-04-21"