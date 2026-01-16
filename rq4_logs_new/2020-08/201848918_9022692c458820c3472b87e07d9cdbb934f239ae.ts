/**
 * @description 套件配置表模块<br>
 * <pre>
 *  object<string, string> type:  设备类型匹配表.
 *    设备分为主设备类型及子设备类型，主类型的key为主类型码，
 *    子类型由主类型码及子类型码组成key <br/>
 *
 *  object<string, string> status: 设备状态匹配表，
 *    分类主类型设备状态，及子类型设备状态，其中主类型状态key由主类型码+状态码组成，
 *    子设备类型状态key由主类型码+子类型码+状态码组成 <br/>
 *
 *  object<string, array> group: 设备类型分组，同组类型状态截码位相同<br/>
 * </pre>
 * @namespace
 * @name Suiter
 */
export declare const Suiter: {
    led: {
        /** led.type 类型*/
        type: {
            '01': string;
            '0101': string;
            '0102': string;
            '0103': string;
            '0111': string;
            '0112': string;
            '0113': string;
            '0114': string;
            '0115': string;
            '0116': string;
            '0119': string;
            '0120': string;
            '0121': string;
            '0122': string;
            '0132': string;
            '0133': string;
        };
        /** led.status 状态*/
        status: {};
        /** led.group 状态分组*/
        group: {
            root: string[];
            simple: string[];
            color: string[];
            way: string[];
        };
    };
    /**
     * 电饭煲
     * @namespace Suiter.cooker
    */
    cooker: {
        type: {
            '02': string;
        };
    };
    /**
     * 加湿器
     * @namespace Suiter.humidifier
    */
    humidifier: {
        type: {
            '03': string;
        };
    };
    /**
     * 插座开关
     * @namespace Suiter.socketSwitch
    */
    socketSwitch: {
        type: {
            '04': string;
            '0401': string;
            '0402': string;
            '0403': string;
            '0412': string;
            '0413': string;
            '0414': string;
            '0421': string;
            '0422': string;
            '0423': string;
            '0424': string;
            '0425': string;
            '0426': string;
            '0427': string;
            '0428': string;
            '0431': string;
            '0432': string;
            '0433': string;
            '0434': string;
            '0436': string;
            '0441': string;
            '0442': string;
            '0443': string;
            '0451': string;
            '0452': string;
            '0462': string;
            '0480': string;
            '0481': string;
            '0483': string;
            '0486': string;
            '0490': string;
            '0491': string;
            '0492': string;
        };
        status: {
            '0400': string;
            '0401': string;
            '0410': string;
            '0411': string;
            '04s1': string;
            '04s0': string;
        };
        group: {
            root: string[];
            touch: string[];
            scene: string[];
            mix: string[];
            normal: string[];
            simple: string[];
        };
        statusLength: {
            '01': number;
            12: number;
            13: number;
            14: number;
            21: number;
            22: number;
            23: number;
            24: number;
            41: number;
            42: number;
            43: number;
            51: number;
            52: number;
            62: number;
            71: number;
            81: number;
            83: number;
        };
    };
    /**
     * 开关类设备
     * @namespace Suiter.switchgear
    */
    switchgear: {
        type: {
            '05': string;
            "0501": string;
            "0502": string;
        };
        status: {
            '0500': string;
            '0501': string;
            '0510': string;
            '0502': string;
        };
    };
    /**
     * 智能风扇
     * @namespace Suiter.fans
    */
    fans: {
        type: {
            '06': string;
        };
    };
    /**
     * 智能空气净化器
     * @namespace Suiter.airCleaner
    */
    airCleaner: {
        type: {
            '07': string;
        };
    };
    tv: {
        type: {
            '08': string;
        };
    };
    usbRf: {
        type: {
            '09': string;
        };
    };
    gateway: {
        type: {
            10: string;
            1001: string;
            1002: string;
        };
    };
    sensors: {
        type: {
            11: string;
            1101: string;
            1102: string;
            1103: string;
            1104: string;
            1105: string;
            1106: string;
            1107: string;
            1108: string;
            1109: string;
            1110: string;
            1111: string;
            1112: string;
            1113: string;
            1114: string;
            1115: string;
            1116: string;
            1117: string;
            1118: string;
            1119: string;
            1120: string;
            1121: string;
            1122: string;
            1123: string;
            1124: string;
            1125: string;
            1126: string;
            1127: string;
            1128: string;
            1129: string;
        };
        status: {
            1100: string;
            1101: string;
            '11fe': string;
            '11fd': string;
            '11ff': string;
            110200: string;
            110202: string;
            110201: string;
            112800: string;
            112802: string;
            112801: string;
            112700: string;
            112702: string;
            112701: string;
            112600: string;
            112602: string;
            112601: string;
            112500: string;
            112502: string;
            112501: string;
            111900: string;
            111901: string;
            111902: string;
            111903: string;
            112400: string;
            112401: string;
            112300: string;
            112301: string;
            112302: string;
            112303: string;
            112100: string;
            112101: string;
            112102: string;
            112103: string;
        };
        group: {
            root: string[];
            als: string[];
            water: string[];
            radar: string[];
            co: string[];
            env: string[];
            body: string[];
            electric: string[];
            virtualRadar: string[];
            light: string[];
            humidifier: string[];
            smoke: string[];
            wave: string[];
            radarSence: string[];
            cardSense: string[];
            envSense: string[];
            induction: string[];
            dc: string;
            ac: string[];
            pm: string[];
            gate: string[];
            radarLight: string[];
            acdcman: string[];
            gas: string[];
            Bedwetting: string[];
            help: string[];
        };
    };
    meterReader: {
        type: {
            12: string;
            1201: string;
        };
    };
    wireControlPanel: {
        type: {
            13: string;
            1301: string;
        };
        group: {
            root: string[];
            ac: string[];
        };
        status: {
            1300: string;
            1301: string;
        };
    };
    transponder: {
        type: {
            14: string;
            1401: string;
            1402: string;
            1403: string;
        };
    };
    remoteControl: {
        type: {
            15: string;
            1501: string;
        };
    };
    autoMover: {
        type: {
            16: string;
            1601: string;
            1602: string;
        };
        group: {
            plane: string[];
        };
    };
    camera: {
        type: {
            17: string;
            1701: string;
            1702: string;
        };
        group: {
            root: string[];
            fixed: string[];
            moving: string[];
        };
    };
    finger: {
        type: {
            16: string;
        };
    };
    doorLock: {
        type: {
            21: string;
            2101: string;
            2102: string;
            2103: string;
            2104: string;
        };
        status: {
            2100: string;
            2101: string;
            2102: string;
            2103: string;
            2104: string;
            2105: string;
            '21-1': string;
            '21open0': string;
            '21open1': string;
            '21open2': string;
            '21open3': string;
            '21open4': string;
            '21open5': string;
            '21close4': string;
            '21close5': string;
            '21close7': string;
            '21close8': string;
            '21close9': string;
            '21card': string;
            default: string;
        };
        group: {
            root: string[];
        };
    };
    remoteControlLamp: {
        type: {
            22: string;
            2201: string;
        };
    };
    smartCamera: {
        type: {
            32: string;
        };
    };
    wifiSocket: {
        type: {
            80: string;
        };
    };
    wifiIr: {
        type: {
            81: string;
        };
    };
    obox: {
        type: {
            10: string;
            1010: string;
        };
        satus: {
            100: string;
            101: string;
        };
        group: {
            root: string[];
        };
    };
};
/**
 * 套件类型
 * @const
 * @memberof Suiter
 * @name SuitTypes
 */
export declare const SuitTypes: string;
/**
 * 套件状态，整合所有设备状态
 * @const
 * @memberof Suiter
 * @name SuitStatus
 */
export declare const SuitStatus: string;
export default Suiter;