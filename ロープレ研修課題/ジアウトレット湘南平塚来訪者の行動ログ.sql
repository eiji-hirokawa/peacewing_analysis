declare polygon_the_outlet_shonanhiratsuka geography;
declare mesh_the_outlet_shonanhiratsuka array<int64>;

-- ポリゴンの指定
set polygon_the_outlet_shonanhiratsuka = ST_GeogFromText('POLYGON((139.36174518323728 35.39326583710515,139.36173445440122 35.38882269444102,139.36292535520383 35.38897138624475,139.3643415615637 35.39113175976157,139.3672061607916 35.39122796938196,139.36716324544736 35.393318313705464,139.36174518323728 35.39326583710515))');

-- メッシュの指定
set mesh_the_outlet_shonanhiratsuka = `prd-analysis.master_candidate.BW_CONVERT_POLYGON_TO_MESH`(polygon_the_outlet_shonanhiratsuka);

-- テーブルの作成
CREATE OR REPLACE TABLE `prd-analysis.w_e_hirokawa.role_playing_shonan` AS

-- ジアウトレット湘南平塚の来訪者のhashed_adidを取得
with visitor_the_outlet_shonanhiratsuka_weekday as (
  select distinct
    hashed_adid,
    1 as weekday_flag
  from
    `prd-analysis.master_v.sdk_master_table_bq`
  where
    -- 計算を軽くするために日付とメッシュで絞る
    date(sdk_detect_ptime, 'Asia/Tokyo') between "2024-04-15" and "2024-04-19" and
    cast(mesh as int64) in unnest(mesh_the_outlet_shonanhiratsuka) and

    -- 時間と場所を指定する
    ST_COVERS(polygon_the_outlet_shonanhiratsuka, st_geogpoint(longitude, latitude)) and

    -- 不正なhashed_adidの除外
    hashed_adid not in (
      '9f89c84a559f573636a47ff8daed0d33', #00000000-0000-0000-0000-000000000000
      'd41d8cd98f00b204e9800998ecf8427e', #空白
      '37a6259cc0c1dae299a7866489dff0bd', #文字列null
      '81684c2e68ade2cd4bf9f2e8a67dd4fe'  #ALLnull
    ) and
    hashed_adid is not null
),

-- ジアウトレット湘南平塚の来訪者のhashed_adidを取得
visitor_the_outlet_shonanhiratsuka_holiday as (
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
    cast(mesh as int64) in unnest(mesh_the_outlet_shonanhiratsuka) and

    -- 時間と場所を指定する
    ST_COVERS(polygon_the_outlet_shonanhiratsuka, st_geogpoint(longitude, latitude)) and

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
    visitor_the_outlet_shonanhiratsuka_weekday
  union all
  select distinct
    hashed_adid
  from
    visitor_the_outlet_shonanhiratsuka_holiday
),

visitor_the_outlet_shonanhiratsuka as(
  select distinct
    a.hashed_adid,
    b.weekday_flag,
    c.holiday_flag
  from
    visitor_hashed_adid a
  left join
    visitor_the_outlet_shonanhiratsuka_weekday b
    on
      a.hashed_adid = b.hashed_adid
  left join
    visitor_the_outlet_shonanhiratsuka_holiday c
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

-- ジアウトレット湘南平塚の来訪者の行動ログを取得
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
  visitor_the_outlet_shonanhiratsuka a
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