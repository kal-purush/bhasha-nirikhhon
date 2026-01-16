/** 各種設定
 *
 */
var Config = function(){};

//Google Drive Api Keyで生成したjsonファイル
Config.prototype.GOOGLE_SPREADSHEET_KEY_JSON = './XXXXXXXX.json';

//Google Drive の出力先シートID
Config.prototype.GOOGLE_SPREADSHEET_ID = 'XXXXXXXXXXXXXXX';


//Google Drive の出力先 ワークシートタイトル
Config.prototype.GOOGLE_SPREADSHEET_WORK_SHEET_TITLE = 'sheet1';

//シートのヘッダー 時間,値,単位
Config.prototype.GOOGLE_SPREADSHEET_WORK_SHEET_HEADER_ROW = ['time','A'];


//BLE の接続先デバイス名
Config.prototype.BLE_B35_LOCAL_NAME = 'OWON-B35';

//B35のサービスUUID
Config.prototype.BLE_B35_SERVICE_UUID = 'fff0';

//値を取得する為のキャラクタリスティック
Config.prototype.BLE_B35_NOTIFY_VALUE_CHARACTERISTIC = 'fff4';

module.exports = new Config();