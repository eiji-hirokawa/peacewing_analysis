declare polygon_estadium, polygon_peacewing, polygon_central_town, polygon_outlet geography;
declare mesh_estadium, mesh_peacewing array<int64>;

-- ポリゴンの指定
set polygon_estadium = ST_GeogFromText('POLYGON((132.39298092551726 34.441847301913235,132.39302920527953 34.439462710868554,132.395813338237 34.439542345805215,132.39562021918792 34.44191808650709,132.39298092551726 34.441847301913235))'); -- Eスタのポリゴン
set polygon_peacewing = ST_GeogFromText('POLYGON((132.453644770067 34.4022758198791,132.45322098104265 34.40123124589024,132.45428850023058 34.40089485488063,132.454723018091 34.40201467760443,132.453644770067 34.4022758198791))'); -- ピースウイングのポリゴン
set polygon_central_town = ST_GeogFromText('POLYGON((132.45572981795542 34.40006104636204,132.45276865920297 34.39042897099984,132.46426997145883 34.38759579669855,132.47916159590952 34.39598879682158,132.47392592391245 34.39942366386291,132.45572981795542 34.40006104636204))'); -- 広島市街地のポリゴン
set polygon_outlet = ST_GeogFromText('POLYGON((132.39666310535722 34.41395139535424,132.39400235401445 34.408428169497725,132.39511815296464 34.40435632717068,132.39773598896318 34.40584345777619,132.4018129466658 34.41267683714338,132.39666310535722 34.41395139535424))'); --ジ・アウトレット広島のポリゴン

-- メッシュの指定
set mesh_estadium = `prd-analysis.master_candidate.BW_CONVERT_POLYGON_TO_MESH`(polygon_estadium);
set mesh_peacewing = `prd-analysis.master_candidate.BW_CONVERT_POLYGON_TO_MESH`(polygon_peacewing);

CREATE OR REPLACE TABLE `prd-analysis.w_e_hirokawa.compare_edion_and_peacewing_before_match_day` AS
-- 2023年J1開幕川 広島vs札幌
-- 2023年2月18日(土) 14:03キックオフ
-- 0-0で引き分け
with e_stadium_2023_02_18 as (
  select distinct
    hashed_adid,
  from
    `prd-analysis.master_v.sdk_master_table_bq`
  where
    -- 計算を軽くするために日付とざっくりの場所で絞る
    date(sdk_detect_ptime, 'Asia/Tokyo') = "2023-02-18" and
    cast(mesh as int64) in unnest(mesh_estadium) and

    -- エディオンスタジアムでの試合時間と場所を指定する
    datetime(sdk_detect_ptime, 'Asia/Tokyo') between '2023-02-18 13:30:00' and '2023-02-18 16:30:00' and
    ST_COVERS(polygon_estadium, st_geogpoint(longitude, latitude)) and

    -- 不正なhashed_adidの除外
    hashed_adid not in (
      '9f89c84a559f573636a47ff8daed0d33', #00000000-0000-0000-0000-000000000000
      'd41d8cd98f00b204e9800998ecf8427e', #空白
      '37a6259cc0c1dae299a7866489dff0bd', #文字列null
      '81684c2e68ade2cd4bf9f2e8a67dd4fe'  #ALLnull
    ) and
    hashed_adid is not null
),

-- 2024年J1開幕戦、広島-浦和戦
-- 2024年2月23日(金・祝) 14:06キックオフ
-- 2-0で広島勝ち
peace_wing_2024_02_24 as (
  select distinct
    hashed_adid
  from
    `prd-analysis.master_v.sdk_master_table_bq`
  where
    -- 計算を軽くするために日付とざっくりの場所で絞る
    date(sdk_detect_ptime, 'Asia/Tokyo') = "2024-02-23" and
    cast(mesh as int64) in unnest(mesh_peacewing) and

    -- エディオンピースウイング広島での試合時間と場所を指定する
    datetime(sdk_detect_ptime, 'Asia/Tokyo') between '2024-02-23 13:30:00' and '2024-02-23 16:30:00' and
    ST_COVERS(polygon_peacewing, st_geogpoint(longitude, latitude)) and

    -- 不正なhashed_adidの除外
    hashed_adid not in (
      '9f89c84a559f573636a47ff8daed0d33', #00000000-0000-0000-0000-000000000000
      'd41d8cd98f00b204e9800998ecf8427e', #空白
      '37a6259cc0c1dae299a7866489dff0bd', #文字列null
      '81684c2e68ade2cd4bf9f2e8a67dd4fe'  #ALLnull
    ) and
    hashed_adid is not null
),

chase_2023_02_17 as (
  select
    a.*
  from
    `prd-analysis.master_v.sdk_master_table_bq` a
  inner join e_stadium_2023_02_18 b
    on a.hashed_adid = b.hashed_adid
  where
    date(sdk_detect_ptime, 'Asia/Tokyo') = "2023-02-17" and
    (longitude > 133 or longitude < 131)
),

chase_2024_02_23 as (
  select
    a.*
  from
    `prd-analysis.master_v.sdk_master_table_bq` a
  inner join peace_wing_2024_02_24 b
    on a.hashed_adid = b.hashed_adid
  where
    date(sdk_detect_ptime, 'Asia/Tokyo') = "2024-02-23" and
    (longitude > 133 or longitude < 131)
),

unionized_table as (
  select
    *
  from
    chase_2023_02_17
  union all
    select
      *
    from chase_2024_02_23
)

select
  a.*,
  date(a.sdk_detect_ptime, 'Asia/Tokyo') as date,
  datetime(a.sdk_detect_ptime, 'Asia/Tokyo') as datetime,
  case when ST_COVERS(polygon_central_town, st_geogpoint(a.longitude, a.latitude)) then 1 end as central_town_flag,
  case when ST_COVERS(polygon_outlet, st_geogpoint(a.longitude, a.latitude)) then 1 end as outlet_hiroshima_flag
from unionized_table a