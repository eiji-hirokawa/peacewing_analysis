/*
滞在判定テーブルの作成
以下リポジトリに滞在判定テーブル作成クエリは集約したため、そちらを参照のこと。
profile_passport/datadev_verification/滞在判定
*/
DECLARE _start_date, _end_date, _target_date DATE;
DECLARE _backlog_num, od_type, _permission, _visiting_minutes, randam_sampling_rate, _short_diffminutes INT64;
DECLARE _anken_name, _table_name, _bucket_name STRING;
DECLARE _pref_array_inner, _city_array_inner, _chome_array_inner, _pref_array_outer, _city_array_outer, _chome_array_outer ARRAY<STRING>;
DECLARE _polygon_inner, _polygon_outer GEOGRAPHY;
DECLARE _need_poihome, _need_poiwork, _need_demography, _need_actionlog, _need_velocity, _need_altitude, _need_trip_purpose, _need_trip_mode,_need_mesh BOOLEAN;

/*
 * ここから案件ごとに変更
 * prd-analysis.li_delivery.{backlog_num}_{anken_name}_odという名前のテーブルが最終的に出力される
 * 日付が連続しない場合、お手数ですが_start_date, _end_dateを変更し、それに合わせanken_nameをどちらの期間なのかわかるように変更して再度実行してください。
 */
# 必須記入項目
SET _start_date = '2024-04-13'; # 抽出対象データの初日
SET _end_date = '2024-04-14'; # 抽出対象データの最終日
SET _permission = 2; # 0: 3rdNG 1: 3rdOK 2: 3rdOK&radiko  基本2で良い。0は集計値を出したい場合
SET _backlog_num = NULL; # バックログのチケットNo.
SET _anken_name = 'role_playing_the_outlet_shonan_hiratsuka_holiday'; # 案件の名前
SET _bucket_name = 'prd-analysis_lifecycle_30days'; # GCSバケット
SET od_type = 2; # 1:内内, 2:内外OR外内, 3:外外
SET _visiting_minutes = 15; # 滞在時間閾値。15分推奨
SET _short_diffminutes = 5; # 移動時間閾値。滞在間の移動がこの分数以下だった場合滞在が連結される。5分推奨
# 選択項目。選択しない項目はNULLにしてください。
## 内の地理条件。meshかpolygonのどちらかは必ず記入すること。どちらも記入しても良い。
### master_mesh_address_statsベースで指定する場合。[txt1, txt2, ...]といったようなarray形式で記載すること　この辺今後変更可能性あり
SET _pref_array_inner = NULL; # 都道府県で指定する場合
SET _city_array_inner = NULL; # 市区町村で指定する場合
SET _chome_array_inner = NULL; # 丁目で指定する場合 小さすぎるので基本指定しない
### polygonベースで指定する場合
SET _polygon_inner = ST_GeogFromText('POLYGON((139.36174518323728 35.39326583710515,139.36173445440122 35.38882269444102,139.36292535520383 35.38897138624475,139.3643415615637 35.39113175976157,139.3672061607916 35.39122796938196,139.36716324544736 35.393318313705464,139.36174518323728 35.39326583710515))'); # 複数ポリゴンがある場合はST_UNION関数を使ってマルチポリゴンにしてください。
## 外の地理条件(内内の場合や、データ量を減らす必要がない場合は不要)
SET _pref_array_outer = NULL;
SET _city_array_outer = NULL;
SET _chome_array_outer = NULL; # 小さすぎるので基本指定しない
SET _polygon_outer = NULL;
## 移動目的オプション
SET _need_trip_purpose = TRUE;
## 移動手段オプション
SET _need_trip_mode = TRUE;
## 居住地/勤務地オプション
SET _need_poihome = TRUE;
SET _need_poiwork = TRUE;
## 属性データオプション
SET _need_demography = TRUE;
## 行動ログデータオプション
SET _need_actionlog = TRUE;
SET _need_velocity = TRUE;
SET _need_altitude = TRUE;
SET _need_mesh = TRUE;
## ランダムサンプリングする場合(統計的信頼性が下がるため非推奨)
SET randam_sampling_rate = NULL;
/*
 * ここまで案件ごとに変更
 */

SET _table_name = CASE WHEN _backlog_num IS NULL THEN _anken_name ELSE CONCAT(_backlog_num, '_', _anken_name) END;

# デバッグ
ASSERT (
  NOT(_permission = 0 AND _need_actionlog IS TRUE)
) AS '行動ログを抽出する際は必ず_permissionは1か2に設定してください。';

SET _table_name = CASE WHEN _backlog_num IS NULL THEN _anken_name ELSE CONCAT(_backlog_num, '_', _anken_name) END;

# 短距離トリップ削除
CALL `prd-analysis.data_dev_team.create_stay_master_table_v2_1_remove_short_trip_for_associate`(_table_name, _permission, _short_diffminutes, _start_date, _end_date, _polygon_inner , _pref_array_inner, _city_array_inner, _chome_array_inner);

# 移動目的付与
IF _need_trip_purpose IS TRUE THEN
  CALL `prd-analysis.data_dev_team.stay_master_table_with_purpose_20240509_for_associate`(_table_name);
END IF;

# 移動手段付与
IF _need_trip_mode IS TRUE THEN
  CALL `prd-analysis.data_dev_team.mode_segment_devide_20240509`(_table_name,_need_trip_purpose,1.8, 0.015, 600, 60, 50, 4, 4);
  CALL `prd-analysis.data_dev_team.stay_and_move_master_table_20240509`(_table_name, _need_trip_purpose);
END IF;


# OD作成
CALL `prd-analysis.data_dev_team.create_od_for_associate`(_table_name, od_type, _visiting_minutes, _start_date, _end_date, _polygon_inner, _pref_array_inner, _city_array_inner, _chome_array_inner, _polygon_outer, _pref_array_outer, _city_array_outer, _chome_array_outer, _need_poihome, _need_poiwork,_need_trip_mode, _need_trip_purpose,_permission);