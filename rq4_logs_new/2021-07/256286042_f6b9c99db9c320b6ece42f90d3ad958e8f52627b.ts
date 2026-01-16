/*
Copyright 2011-2019, RFXCOM
ALL RIGHTS RESERVED.
The RFXtrx protocol is owned by RFXCOM, and is protected under
Netherlands Copyright Laws and Treaties and shall be subject to the 
exclusive jurisdiction of the Netherlands Courts. The information from this
file may freely be used to create programs to exclusively interface with
RFXCOM products only.
The above copyright notice shall be included in all copies or substantial
portions of this file.
'----------------------------------------------------------------------------
*/
import { uint8, uint16, uint32, int8, int16, int32, float, double, uint64, parsers } from '@domojs/protocol-parser';
export { uint8, uint16, uint32, int8, int16, int32, float, double, uint64 }
import { log as debug } from '@akala/core';

const log = debug('rfxtrx');

export enum PacketType
{
    INTERFACE_CONTROL = 0x0,
    INTERFACE_MESSAGE = 0x1,
    TRANSMITTER_MESSAGE = 0x2,
    UNDECODED_RF_MESSAGE = 0x3,

    LIGHTING1 = 0x10,
    LIGHTING2 = 0x11,
    LIGHTING3 = 0x12,
    LIGHTING5 = 0x14,
    LIGHTING4 = 0x13,
    LIGHTING6 = 0x15,
    CHIME = 0x16,
    FAN = 0x17,
    CURTAIN1 = 0x18,
    BLINDS1 = 0x19,
    RFY = 0x1A,
    HOMECONFORT = 0x1B,
    EDISIO = 0x1C,
    HONEYWELL_ACTIVLINK = 0x1D,
    FUNKBUS = 0x1E,

    SECURITY1 = 0x20,
    SECURITY2 = 0x21,
    CAMERA1 = 0x28,

    REMOTE_CONTROL = 0x30,

    THERMOSTAT1 = 0x40,
    THERMOSTAT2 = 0x41,
    THERMOSTAT3 = 0x42,
    BBQ1 = 0x48,
    BBQ_TEMPERATURE = 0x4E,
    TEMPERATURE_RAIN = 0x4F,

    TEMPERATURE = 0x50,
    HUMIDITY = 0x51,
    TEMPERATURE_HUMIDITY = 0x52,
    BAROMETRIC = 0x53,
    TEMPERATURE_HUMIDITY_BAROMETRIC = 0x54,
    RAIN = 0x55,
    WIND = 0x56,
    UV = 0x57,
    DATE_TIME = 0x58,
    CURRENT = 0x59,
    ENERGY = 0x5A,
    CURRENT_ENERGY = 0x5B,
    POWER = 0x5C,
    WEIGHT = 0x5D,
    GAS = 0x5E,
    WATER = 0x5F,
    RFXSENSOR = 0x5F,

    RFXMETER = 0x70,
    FS20 = 0x72,
    RAW = 0x7F,

    IO_LINES = 0x80,

    RESERVED = 0x90,

    UNKNOWN = 255

}

var messages;

export var Protocol = parsers.sub(parsers.uint8, parsers.object<Message>(
    parsers.property('type', parsers.uint16),
    parsers.property('sequenceNumber', parsers.uint8),
    messages = parsers.chooseProperty<Message, 'type', 'message'>('type', 'message', {})
));

export { messages };
import * as Lighting1 from './lighting1';
import * as Lighting2 from './lighting2';
import * as Lighting3 from './lighting3';
import * as Lighting4 from './lighting4';
import * as Lighting5 from './lighting5';
import * as Lighting6 from './lighting6';
import * as Chime from './chime';
import * as Fan from './fan';
import * as Curtain1 from './curtain1';
import * as Blinds1 from './blinds1';
import * as Rfy from './rfy';
import * as Homeconfort from './homeconfort';
import * as Edisio from './edisio';
import * as Honeywell from './honeywell activelink';
import * as Funkbus from './funkbus';
import * as Security1 from './security1';
import * as InterfaceControl from './0.interface.mode';
import * as InterfaceMessage from './1.interface.response';
import * as Elec1 from './elec1';
import * as Elec2 from './elec2';
import * as TemperatureHumidity from './temperature and humidity';


export namespace Type
{
    export enum INTERFACE_CONTROL
    {
        Mode = PacketType.INTERFACE_CONTROL << 8 | 0x00,
    }
    export enum INTERFACE_MESSAGE
    {
        mode = PacketType.INTERFACE_MESSAGE << 8 | 0x00,
        unknownRTSRemote = PacketType.INTERFACE_MESSAGE << 8 | 0x01,
        noExtendedHardwarePresent = PacketType.INTERFACE_MESSAGE << 8 | 0x02,
        listRFYRemotes = PacketType.INTERFACE_MESSAGE << 8 | 0x03,
        listASARemotes = PacketType.INTERFACE_MESSAGE << 8 | 0x04,
        start = PacketType.INTERFACE_MESSAGE << 8 | 0x07,
        unknown = PacketType.INTERFACE_MESSAGE << 8 | 0xff,

    }
    export enum TRANSMITTER_MESSAGE
    {
        Error = PacketType.TRANSMITTER_MESSAGE << 8 | 0x00,
        Response = PacketType.TRANSMITTER_MESSAGE << 8 | 0x01,
    }
    export enum UNDECODED_RF_MESSAGE
    {
        AC = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x00,
        ARC = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x01,
        Ati = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x02,
        Hideki = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x03,
        UPM = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x03,
        Lacrosse = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x04,
        Viking = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x04,
        AD = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x05,
        Mertik = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x06,
        Oregon1 = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x07,
        Oregon2 = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x08,
        Oregon3 = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x09,
        Proguard = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x0A,
        Visonic = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x0B,
        Nec = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x0C,
        FS20 = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x0D,
        Reserved = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x0E,
        Blinds = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x0F,
        Rubicson = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x10,
        AE = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x11,
        FineOffset = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x12,
        RGB = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x13,
        RTS = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x14,
        SelectPlus = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x15,
        Homeconfort = PacketType.UNDECODED_RF_MESSAGE << 8 | 0x16,
    }

    export enum LIGHTING1
    {
        X10 = PacketType.LIGHTING1 << 8 | Lighting1.SubType.X10,
        ARC = PacketType.LIGHTING1 << 8 | Lighting1.SubType.ARC,
        AB400 = PacketType.LIGHTING1 << 8 | Lighting1.SubType.AB400,
        Waveman = PacketType.LIGHTING1 << 8 | Lighting1.SubType.Waveman,
        Chacon_EMW200 = PacketType.LIGHTING1 << 8 | Lighting1.SubType.Chacon_EMW200,
        IMPULS = PacketType.LIGHTING1 << 8 | Lighting1.SubType.IMPULS,
        RisingSun = PacketType.LIGHTING1 << 8 | Lighting1.SubType.RisingSun,
        PhilipsSBC = PacketType.LIGHTING1 << 8 | Lighting1.SubType.PhilipsSBC,
        Energenie_ENER010 = PacketType.LIGHTING1 << 8 | Lighting1.SubType.Energenie_ENER010,
        Energenie_5gang = PacketType.LIGHTING1 << 8 | Lighting1.SubType.Energenie_5gang,
        COCO_GDR22000R = PacketType.LIGHTING1 << 8 | Lighting1.SubType.COCO_GDR22000R,
        HQ_COCO20 = PacketType.LIGHTING1 << 8 | Lighting1.SubType.HQ_COCO20,
        Oase_Inscenio_FM_Master = PacketType.LIGHTING1 << 8 | Lighting1.SubType.Oase_Inscenio_FM_Master,
    }
    export enum LIGHTING2
    {
        AC = PacketType.LIGHTING2 << 8 | Lighting2.SubType.AC,
        HomeEasyEU = PacketType.LIGHTING2 << 8 | Lighting2.SubType.HomeEasyEU,
        ANSLUT = PacketType.LIGHTING2 << 8 | Lighting2.SubType.ANSLUT,
        KambrookRF3672 = PacketType.LIGHTING2 << 8 | Lighting2.SubType.KambrookRF3672,
    }
    export enum LIGHTING3
    {
        IkeaKoppla = PacketType.LIGHTING3 << 8 | Lighting3.SubType.IkeaKoppla
    }
    export enum LIGHTING4
    {
        PT2262 = PacketType.LIGHTING4 << 8 | Lighting4.SubType.PT2262
    }
    export enum LIGHTING5
    {
        LightwaveRF = PacketType.LIGHTING5 << 8 | Lighting5.SubType.LightwaveRF,
        Siemens = PacketType.LIGHTING5 << 8 | Lighting5.SubType.Siemens,
        EMW100GAO = PacketType.LIGHTING5 << 8 | Lighting5.SubType.EMW100GAO,
        Everflourish = PacketType.LIGHTING5 << 8 | Lighting5.SubType.Everflourish,
        BBSB = PacketType.LIGHTING5 << 8 | Lighting5.SubType.BBSB,
        MDRemote106LedDimmer = PacketType.LIGHTING5 << 8 | Lighting5.SubType.MDRemote106LedDimmer,
        ConradRSL2 = PacketType.LIGHTING5 << 8 | Lighting5.SubType.ConradRSL2,
        OTIO = PacketType.LIGHTING5 << 8 | Lighting5.SubType.OTIO,
        LivoloDimmer = PacketType.LIGHTING5 << 8 | Lighting5.SubType.LivoloDimmer,
        RGB_TRC02_2batt = PacketType.LIGHTING5 << 8 | Lighting5.SubType.RGB_TRC02_2batt,
        AokeRelay = PacketType.LIGHTING5 << 8 | Lighting5.SubType.AokeRelay,
        RGB_TRC02_3batt = PacketType.LIGHTING5 << 8 | Lighting5.SubType.RGB_TRC02_3batt,
        Eurodomest = PacketType.LIGHTING5 << 8 | Lighting5.SubType.Eurodomest,
        LivoloAppliance = PacketType.LIGHTING5 << 8 | Lighting5.SubType.LivoloAppliance,
        RGB432W = PacketType.LIGHTING5 << 8 | Lighting5.SubType.RGB432W,
        MDRemote107LedDimmer = PacketType.LIGHTING5 << 8 | Lighting5.SubType.MDRemote107LedDimmer,
        LegrandCAD = PacketType.LIGHTING5 << 8 | Lighting5.SubType.LegrandCAD,
        Avantek = PacketType.LIGHTING5 << 8 | Lighting5.SubType.Avantek,
        FA500 = PacketType.LIGHTING5 << 8 | Lighting5.SubType.FA500,
        PROMax = PacketType.LIGHTING5 << 8 | Lighting5.SubType.PROMax,
        MDRemote108LedDimmer = PacketType.LIGHTING5 << 8 | Lighting5.SubType.MDRemote108LedDimmer,
        Kangtai = PacketType.LIGHTING5 << 8 | Lighting5.SubType.Kangtai,
        Cotech = PacketType.LIGHTING5 << 8 | Lighting5.SubType.Cotech,
    }
    export enum LIGHTING6
    {
        Blyss = PacketType.LIGHTING6 << 8 | Lighting6.SubType.Blyss,
        Cuveo = PacketType.LIGHTING6 << 8 | Lighting6.SubType.Cuveo,
    }
    export enum CHIME
    {
        ByronSX = PacketType.CHIME << 8 | Chime.SubType.ByronSX,
        ByronMP001 = PacketType.CHIME << 8 | Chime.SubType.ByronMP001,
        SelectPlus = PacketType.CHIME << 8 | Chime.SubType.SelectPlus,
        Envivo = PacketType.CHIME << 8 | Chime.SubType.Envivo,
    }
    export enum FAN
    {
        SiemensSF01 = PacketType.FAN << 8 | Fan.SubType.SiemensSF01,
        IthoCVERFT = PacketType.FAN << 8 | Fan.SubType.IthoCVERFT,
        LucciAirFan = PacketType.FAN << 8 | Fan.SubType.LucciAirFan,
        SEAV = PacketType.FAN << 8 | Fan.SubType.SEAV,
        WestingHouse = PacketType.FAN << 8 | Fan.SubType.WestingHouse,
        LucciAirDC = PacketType.FAN << 8 | Fan.SubType.LucciAirDC,
        CasaFan = PacketType.FAN << 8 | Fan.SubType.CasaFan,
        FT1211RFan = PacketType.FAN << 8 | Fan.SubType.FT1211RFan,
        Falmec = PacketType.FAN << 8 | Fan.SubType.Falmec,
        LucciAirDCII = PacketType.FAN << 8 | Fan.SubType.LucciAirDCII,
    }
    export enum CURTAIN1
    {
        Harrison = PacketType.CURTAIN1 << 8 | Curtain1.SubType.Harrison
    }
    export enum BLINDS1
    {
        BlindsT0 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT0,
        BlindsT1 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT1,
        BlindsT2 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT2,
        BlindsT3 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT3,
        BlindsT4 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT4,
        BlindsT5 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT5,
        BlindsT6 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT6,
        BlindsT7 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT7,
        BlindsT8 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT8,
        BlindsT9 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT9,
        BlindsT10 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT10,
        BlindsT11 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT11,
        BlindsT12 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT12,
        BlindsT13 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT13,
        BlindsT14 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT14,
        // BlindsT15 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT15,
        BlindsT16 = PacketType.BLINDS1 << 8 | Blinds1.SubType.BlindsT16,
    }
    export enum RFY
    {
        Standard = PacketType.RFY << 8 | Rfy.SubType.Standard,
        Extended = PacketType.RFY << 8 | Rfy.SubType.Extended,
        RESERVED = PacketType.RFY << 8 | Rfy.SubType.RESERVED,
        ASA = PacketType.RFY << 8 | Rfy.SubType.ASA,
    }
    export enum HOMECONFORT
    {
        TEL010 = PacketType.HOMECONFORT << 8 | Homeconfort.SubType.Tel010,
    }
    export enum EDISIO
    {
        Controller = PacketType.EDISIO << 8 | Edisio.SubType.Controller,
    }
    export enum HOMECONFORT_ACTIVLINK
    {
        Homeconfort = PacketType.HONEYWELL_ACTIVLINK << 8 | Honeywell.SubType.Series5Chime,
    }
    export enum FUNKBUS
    {
        GiraRemote = PacketType.FUNKBUS << 8 | Funkbus.SubType.GiraRemote,
        InstaRemote = PacketType.FUNKBUS << 8 | Funkbus.SubType.InstaRemote,
    }
    export enum SECURITY1
    {
        X10DoorSensor = PacketType.SECURITY1 << 8 | Security1.SubType.X10DoorSensor,
        X10MotionSensor = PacketType.SECURITY1 << 8 | Security1.SubType.X10MotionSensor,
        X10Remote = PacketType.SECURITY1 << 8 | Security1.SubType.X10Remote,
        KD101 = PacketType.SECURITY1 << 8 | Security1.SubType.KD101,
        VisonicPowerCodeDoorSensor = PacketType.SECURITY1 << 8 | Security1.SubType.VisonicPowerCodeDoorSensor,
        VisonicPowerCodeMotionSensor = PacketType.SECURITY1 << 8 | Security1.SubType.VisonicPowerCodeMotionSensor,
        VisonicCodeSecure = PacketType.SECURITY1 << 8 | Security1.SubType.VisonicCodeSecure,
        VisonicPowerCodeDoorSensorAux = PacketType.SECURITY1 << 8 | Security1.SubType.VisonicPowerCodeDoorSensorAux,
        Meiantech = PacketType.SECURITY1 << 8 | Security1.SubType.Meiantech,
        Atlantic = PacketType.SECURITY1 << 8 | Security1.SubType.Atlantic,
        SA30 = PacketType.SECURITY1 << 8 | Security1.SubType.SA30,
        SA33 = PacketType.SECURITY1 << 8 | Security1.SubType.SA33,
        RM174RF = PacketType.SECURITY1 << 8 | Security1.SubType.RM174RF,
    }
    export enum SECURITY2
    {

    }
    export enum CAMERA1
    {

    }
    export enum REMOTE_CONTROL
    {

    }
    export enum THERMOSTAT1
    {

    }
    export enum THERMOSTAT2
    {

    }
    export enum THERMOSTAT3
    {

    }
    export enum BBQ1
    {

    }
    export enum BBQ_TEMPERATURE
    {

    }
    export enum TEMPERATURE_RAIN
    {

    }
    export enum TEMPERATURE
    {

    }
    export enum HUMIDITY
    {

    }
    export enum TEMPERATURE_HUMIDITY
    {
        TH1 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH1,
        TH2 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH2,
        TH3 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH3,
        TH4 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH4,
        TH5 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH5,
        TH6 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH6,
        TH7 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH7,
        TH8 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH8,
        TH9 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH9,
        TH10 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH10,
        TH11 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH11,
        TH12 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH12,
        TH13 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH13,
        TH14 = PacketType.TEMPERATURE_HUMIDITY << 8 | TemperatureHumidity.SubType.TH14,
    }
    export enum BAROMETRIC
    {

    }
    export enum TEMPERATURE_HUMIDITY_BAROMETRIC
    {

    }
    export enum RAIN
    {

    }
    export enum WIND
    {

    }
    export enum UV
    {

    }
    export enum DATE_TIME
    {

    }
    export enum CURRENT
    {

    }
    export enum ENERGY
    {
        CM119 = PacketType.ENERGY << 8 | Elec2.SubType.CM119,
        CM160 = PacketType.ENERGY << 8 | Elec2.SubType.CM160,
        CM180 = PacketType.ENERGY << 8 | Elec2.SubType.CM180,
    }
    export enum CURRENT_ENERGY
    {
        CM113 = PacketType.CURRENT << 8 | Elec1.SubType.CM113,
        Electrisave = PacketType.CURRENT << 8 | Elec1.SubType.Electrisave,
        CentaMeter = PacketType.CURRENT << 8 | Elec1.SubType.CentaMeter,
    }
    export enum POWER
    {

    }
    export enum WEIGHT
    {

    }
    export enum GAS
    {

    }
    export enum WATER
    {

    }
    export enum RFXSENSOR
    {

    }
    export enum RFXMETER
    {

    }
    export enum FS20
    {

    }
    export enum RAW
    {

    }
    export enum IO_LINES
    {

    }
    export enum RESERVED
    {

    }
    export enum UNKNOWN
    {

    }
}

export type EventMapSimple = { [k in keyof PacketType]: RFXDevice }
export interface EventMap extends AnimationEventMap
{
    INTERFACE_MESSAGE: InterfaceMessage.ModeResponse | InterfaceMessage.ListRFYRemote | InterfaceMessage.CheckRFXCOMDevice;
    CURRENT_ENERGY: Elec1.Device;
    ENERGY: Elec2.Device;
    TEMPERATURE_HUMIDITY: TemperatureHumidity.Device;
}

export interface Message<TSubMessage = any>
{
    type: number;
    sequenceNumber: number;
    message: TSubMessage;
}

export type RFXDevice =
    InterfaceControl.ModeCommand
    | Rfy.Device
    | Blinds1.Device
    | Fan.Device

    | Lighting1.Device
    | Lighting2.Device
    | Lighting3.Device
    | Lighting4.Device
    | Lighting5.Device
    | Lighting6.Device
    | Elec1.Device
    | Elec2.Device
    | TemperatureHumidity.Device
    ;

InterfaceControl.init();
InterfaceMessage.init();
Rfy.init();
Blinds1.init();
Fan.init();
Lighting1.init();
Lighting2.init();
Lighting3.init();
Lighting4.init();
Lighting5.init();
Lighting6.init();
Elec1.init();
Elec2.init();
TemperatureHumidity.init();

export
{
    InterfaceControl, InterfaceMessage, Rfy, Blinds1, Fan, Lighting1, Lighting2, Lighting3, Lighting4, Lighting5, Lighting6
    , Elec1, Elec2, TemperatureHumidity
}