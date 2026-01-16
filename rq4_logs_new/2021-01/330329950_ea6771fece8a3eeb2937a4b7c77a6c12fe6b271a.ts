enum soft_serial {
    //% block="P0"
    serial_0 = 0,
    //% block="P1"
    serial_1 = 1,
    //% block="P2"
    serial_2 = 2,
    //% block="P8"
    serial_8 = 3,
    //% block="P13"
    serial_13 = 4,
    //% block="P14"
    serial_14 = 5,
    //% block="P15"
    serial_15 = 6,
    //% block="P16"
    serial_16 = 7
}


let serial_list = [SerialPin.P0, SerialPin.P1, SerialPin.P2, SerialPin.P8, SerialPin.P13, SerialPin.P14, SerialPin.P15, SerialPin.P16];
let state = "";
let clear_ = "";

let weatherList = ["", "", "", "", "", ""];
let airboxList = ["", "", "", "", "", "", "", ""];
let twaqiList = ["", "", "", "", "", "", "", "", ""];
let twstockList = ["", "", "", "", "", "", "", "", "", ""];

let list_str = ["基隆", "汐止", "萬里", "新店", "土城", "板橋", "新莊", "菜寮", "林口", "淡水", "士林", "中山", "萬華", "古亭", "松山", "大同", "桃園", "大園", "觀音", "平鎮", "龍潭", "湖口", "竹東", "新竹", "頭份", "苗栗", "三義", "豐原", "沙鹿", "大里", "忠明", "西屯", "彰化", "線西", "二林", "南投", "斗六", "崙背", "新港", "朴子", "臺西", "嘉義", "新營", "善化", "安南", "臺南", "美濃", "橋頭", "仁武", "鳳山", "大寮", "林園", "楠梓", "左營", "前金", "前鎮", "小港", "屏東", "潮州", "恆春", "臺東", "花蓮", "陽明", "宜蘭", "冬山", "三重", "中壢", "竹山", "永和", "復興", "埔里", "馬祖", "金門", "馬公", "關山", "麥寮", "富貴角", "彰化(員林)", "彰化(大城)", "臺南(麻豆)", "屏東(琉球)", "臺南(北門)", "新北(樹林)", "永和(環河)", "屏東(枋寮)"];

function check_name(state: string) :number{
    let count = -1;
    for (let i=0; i<list_str.length(); i++) {
        if (state.includes(list_str[i])) {
            count = i;
            break;
        }
    }
    return count;
}

//% weight=0 color=#0000ff icon="\uf0c2" block="logyun"
//% groups=['MQTT']
//% groups=['Data Send']
//% groups=['Data Read']
namespace logyun {
    export enum city_number {
        //% block="Keelung City"
        _data1 = "6724654",
        //% block="Taipei City"
        _data2 = "1668341",
        //% block="New Taipei City"
        _data3 = "1670029",
        //% block="Taoyuan City"
        _data4 = "1667905",
        //% block="Hsinchu City"
        _data5 = "1675107",
        //% block="Miaoli County"
        _data6 = "1675107",
        //% block="Taichung City"
        _data7 = "1668399",
        //% block="Changhua County"
        _data8 = "1679136",
        //% block="Nantou County"
        _data9 = "1671564",
        //% block="Yunlin County"
        _data10 = "1665194",
        //% block="Chiayi City"
        _data11 = "1678836",
        //% block="Tainan City"
        _data12 = "1668352",
        //% block="Kaohsiung City"
        _data13 = "7280289",
        //% block="Pingtung County"
        _data14 = "1670479",
        //% block="Yilan County"
        _data15 = "1674197",
        //% block="Hualien County"
        _data16 = "1674502",
        //% block="Taitung County"
        _data17 = "1668295",
        //% block="Penghu County"
        _data18 = "1670651",
        //% block="Kinmen County"
        _data19 = "1678008",
        //% block="Mazu Nangan"
        _data20 = "7552914"

    }

    export enum open_weather_choose {
        //% block="temp"
        _data1 = 0,
        //% block="temp min"
        _data2 = 1,
        //% block="temp max"
        _data3 = 2,
        //% block="pressure"
        _data4 = 3,
        //% block="Humid"
        _data5 = 4,
        //% block="wind speed"
        _data6 = 5
    }

    export enum air_box_choose {
        //% block="gps_lon"
        _data1 = 1,
        //% block="gps_lat"
        _data2 = 2,
        //% block="s_d2"
        _data3 = 3,
        //% block="s_d0"
        _data4 = 4,
        //% block="s_d1"
        _data5 = 5,
        //% block="s_t0"
        _data6 = 6,
        //% block="s_h0"
        _data7 = 7
    }

    export enum tw_aqi_choose {
        //% block="AQI"
        _data1 = 0,
        //% block="SO2"
        _data2 = 1,
        //% block="CO"
        _data3 = 2,
        //% block="O3"
        _data4 = 3,
        //% block="WindSpeed"
        _data5 = 4,
        //% block="PM10"
        _data6 = 5,
        //% block="PM2.5"
        _data7 = 6,
        //% block="PM10_AVG"
        _data8 = 7,
        //% block="PM2.5_AVG"
        _data9 = 8
    }

    export enum tw_stock_choose {
        //% block="Stock ID"
        _data1 = 0,
        //% block="Opening Price"
        _data2 = 1,
        //% block="Closing Price"
        _data3 = 2,
        //% block="Day Hight"
        _data4 = 3,
        //% block="Day Low"
        _data5 = 4,
        //% block="Volume"
        _data6 = 5,
        //% block="Total Volume"
        _data7 = 6,
        //% block="Strike price"
        _data8 = 7,
        //% block="date"
        _data9 = 8,
        //% block="time"
        _data10 = 9
    }
    //% blockId=connect_logyun block="Logyun connect WiFi|RX %choose1|TX %choose2|Wi-Fi SSID: %ssid|Password: %key"
    //% weight=10
    export function connect_logyun(choose1: soft_serial, choose2: soft_serial, ssid: string, key: string): void {
        serial.redirect(serial_list[choose1], serial_list[choose2] ,BaudRate.BaudRate115200);
        basic.pause(1000);
        if (connecting_logyun()) {
            clear_ = serial.readString();
        }
        else {
            clear_ = serial.readString();
            serial.writeLine("WifiConnect(YisrealWifi,0958002618)");
            state = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        }

    }

    //% blockId=connecting_logyun block="logyun Wifi connecting ?"
    //% weight=10
    export function connecting_logyun(): boolean {
        clear_ = serial.readString();
        serial.writeLine("WifiCheck()");
        state = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        if (state == "ok") {
            return true;
        }
        else {
            return false;
        }
    }

    //% expandableArgumentMode"toggle" inlineInputMode=inline
    //% blockId=logyun_thingspeak block="Upload ThingSpeak API Keys %apikey|Field1 %f1||Field2 %f2 Field3 %f3 Field4 %f4 Field5 %f5 Field6 %f6 Field7 %f7 Field8 %f8"
    //% weight=10
    //% group="Data Send"
    export function logyun_thingspeak(apikey: string, f1: number, f2?: number, f3?: number, f4?: number, f5?: number, f6?: number, f7?: number, f8?: number): void {
        let datalist = [f1,f2,f3,f4,f5,f6,f7,f8];
        let sendText = "ThingSpeak("+apikey;
        for (let i=0; i<datalist.length; i++) {
            if (datalist[i] != null) {
                sendText += "," + datalist[i];
            }
            else {
                sendText += ")"
                break;
            }
        }
        clear_ = serial.readString();
        serial.writeLine(sendText);
    }

    //% expandableArgumentMode"toggle" inlineInputMode=inline
    //% blockId=logyun_googlesheet block="Upload Google Sheet Spreadsheet Key %apikey|Field1 %f1||Field2 %f2 Field3 %f3 Field4 %f4 Field5 %f5 Field6 %f6 Field7 %f7 Field8 %f8 Field9 %f9 Field10 %f10"
    //% weight=10
    //% group="Data Send"
    export function logyun_googlesheet(apikey: string, f1: string, f2?: string, f3?: string, f4?: string, f5?: string, f6?: string, f7?: string, f8?: string, f9?: string, f10?: string): void {
        let datalist = [f1,f2,f3,f4,f5,f6,f7,f8,f9,f10];
        let sendText = "GoogleSheet("+apikey;
        for (let i=0; i<datalist.length; i++) {
            if (datalist[i] != null) {
                sendText += "," + datalist[i];
            }
            else {
                sendText += ")";
                break;
            }
        }
        clear_ = serial.readString();
        serial.writeLine(sendText);
    }

    //% blockId="open_weather" block="Connect OpenWeather API Keys %apikey choose %city_number"
    //% weight=10
    //% group="Data Read"
    export function open_weather(apikey: string, city_number: city_number): void {
        let list = ["0", "0", "0", "0", "0", "0"];
        let sendText = "OpenWeather("+city_number+","+apikey+")";
        clear_ = serial.readString();
        serial.writeLine(sendText);
        for (let i=0; i<list.length; i++) {
            list[i] = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        }
        for (let i=0; i<list.length; i++) {
            weatherList[i] = list[i];
        }
    }

    //% blockId="open_weather_data" block="Get OpenWeather %choose data"
    //% weight=10
    //% group="Data Read"
    export function open_weather_data(choose: open_weather_choose): number {
        return parseFloat(weatherList[choose]);
    }

    //% blockId="air_box" block="Connect AirBox Divice ID %apikey"
    //% weight=10
    //% group="Data Read"
    export function air_box(apikey: string): void {
        let list = ["0", "0", "0", "0", "0", "0", "0", "0"]
        let sendText = "AirBox("+apikey+")";
        clear_ = serial.readString();
        serial.writeLine(sendText);
        for (let i=0; i<list.length; i++) {
            list[i] = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        }
        for (let i=0; i<list.length; i++) {
            airboxList[i] = list[i];
        }
    }

    //% blockId="air_box_data" block="Get AirBox %choose data"
    //% weight=10
    //% group="Data Read"
    export function air_box_data(choose: air_box_choose): number {
        if (choose < 3) {
            return parseFloat(airboxList[choose]);
        }
        else {
            return parseInt(airboxList[choose]);
        }
    }

    //% blockId="tw_aqi" block="Connect Taiwan AQI region %region"
    //% weight=10
    //% group="Data Read"
    export function tw_aqi(region: string): void {
        let list = ["0", "0", "0", "0", "0", "0", "0", "0", "0"];
        let target = check_name(region);
        if (target != -1) {
            clear_ = serial.readString();
            serial.writeLine("TaiwanAQI("+target+")");
            for (let i=0; i<list.length; i++) {
                list[i] = serial.readUntil(serial.delimiters(Delimiters.NewLine));
            }
            for (let i=0; i<list.length; i++) {
                twaqiList[i] = list[i];
            }
        }
    }

    //% blockId="tw_aqi_data" block="Get Taiwan AQI %choose data"
    //% weight=10
    //% group="Data Read"
    export function tw_aqi_data(choose: tw_aqi_choose): number {
        if (choose > 0 && choose < 5) {
            return parseFloat(twaqiList[choose]);
        }
        else {
            return parseInt(twaqiList[choose]);
        }
    }

    //% blockId="tw_stock" block="Connect Taiwan Stock code %scode"
    //% weight=10
    //% group="Data Read"
    export function tw_stock(scode: string): void {
        let list = ["0", "0", "0", "0", "0", "0", "0", "0", "0", "0"];
        let sendText = "TaiwanStock("+scode+")";
        clear_ = serial.readString();
        serial.writeLine(sendText);
        for (let i=0; i<list.length; i++) {
            list[i] = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        }
        for (let i=0; i<list.length; i++) {
            twstockList[i] = list[i];
        }
    }

    //% blockId="tw_stock_data" block="Get Taiwan Stock %choose data"
    //% weight=10
    //% group="Data Read"
    export function tw_stock_data(choose: tw_stock_choose) {
        if ((choose > 0 && choose < 5) || choose == 7) {
            return parseFloat(twstockList[choose]);
        }
        else if (choose == 9){
            return twstockList[choose];
        }
        else {
            return parseInt(twstockList[choose]);
        }
    }

    //% blockId="mqtt_connect" block="MQTT Connect|Server: %server_ip|Port: %server_port|User ID: %user_id|User Name: %user_name|User password: %user_password|Subscribe Topic: %topic"
    //% weight=10
    //% group="MQTT"
    export function mqtt_connect(server_ip: string, server_port: string, user_id: string, user_name: string, user_password: string, topic: string): void {
        let sendText = "MQTTConnect("+""+server_ip+","+""+server_port+","+""+user_id+","+""+user_name+","+""+user_password+","+""+topic+")";
        clear_ = serial.readString();
        serial.writeLine(sendText);
        state = serial.readUntil(serial.delimiters(Delimiters.NewLine));
    }

    //% blockId="mqtt_received_topic" block="MQTT Received Topic"
    //% weight=10
    //% group="MQTT"
    export function mqtt_received_topic(): string {
        let sendText = "MQTTReceivedTopic()";
        clear_ = serial.readString();
        serial.writeLine(sendText);
        state = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        return state;
    }

    //% blockId="mqtt_received_msg" block="MQTT Received Msg"
    //% weight=10
    //% group="MQTT"
    export function mqtt_received_msg(): string {
        let sendText = "MQTTReceivedMsg()";
        clear_ = serial.readString();
        serial.writeLine(sendText);
        state = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        return state;
    }

    //% blockId="mqtt_connectd_check" block="MQTT Connectd Check"
    //% weight=10
    //% group="MQTT"
    export function mqtt_connectd_check(): boolean {
        let sendText = "MQTTConnectdCheck()";
        clear_ = serial.readString();
        serial.writeLine(sendText);
        state = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        if (state == "ok") {
            return true;
        }
        else {
            return false;
        }
    }

    //% blockId="mqtt_break" block="MQTT Break"
    //% weight=10
    //% group="MQTT"
    export function mqtt_break(): string {
        let sendText = "MQTTBreak()";
        clear_ = serial.readString();
        serial.writeLine(sendText);
        state = serial.readUntil(serial.delimiters(Delimiters.NewLine));
        return state;
    }
}