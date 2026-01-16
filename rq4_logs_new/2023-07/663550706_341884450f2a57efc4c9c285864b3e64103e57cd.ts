import { Characteristic } from "react-native-ble-plx";
import { base64ToHex, hexToBase64 } from "../utils";

export const CS108_NAME_REGEX = "CS108";
export const CS108_EID_SERVICE_PREFIX = "00009800";
export const CS108_EID_SERVICE_CHARACTERISTIC_READ_PREFIX = "00009901";
export const CS108_EID_SERVICE_CHARACTERISTIC_WRITE_PREFIX = "00009900";

export const isCS108Device = (deviceName: string | null) => {
  if (!deviceName) {
    return false;
  }
  const regEx = new RegExp(CS108_NAME_REGEX, "gi");
  return regEx.test(deviceName);
};

const NOTIFICATION = {
  TRIGGER_PUSHED: "A102",
  TRIGGER_RELEASED: "A103",
};

const EVENT_CODE = {
  TAG_READ: "8100",
};

// Header values
const PREFIX = "A7"; // Byte 1
const CONNECTION = "B3"; // Byte 2
// Byte 3 is payload length (number of bytes in the payload)
const DESTINATION = {
  // Byte 4
  RFID: "C2",
  NOTIFICATION: "D9",
};
const RESERVED = "82"; // Byte 5 (but is a sequence number when uplink for RFID)
const DIRECTION = {
  // Byte 6
  Downlink: "D9",
  Uplink: "9E",
};
const DOWNLINK_CRC1 = "00";
const DOWNLINK_CRC2 = "00";
const DOWNLINK_PAYLOAD_LENGTH = "0A"; // 10 bytes
//CRC1 and CRC2 are bytes 7 and 8

const defaultCommandHeader =
  PREFIX +
  CONNECTION +
  DOWNLINK_PAYLOAD_LENGTH +
  DESTINATION.RFID +
  RESERVED +
  DIRECTION.Downlink +
  DOWNLINK_CRC1 +
  DOWNLINK_CRC2;

const RFID_COMMAND = "8002";
const START_INVENTORY = RFID_COMMAND + "700100f00f000000"; // Start inventory operation
const ABORT_INVENTORY = RFID_COMMAND + "4003000000000000";
const TURN_ON_RFID = "8000"; // Turn on RFID module
const ANT_PORT_SEL = RFID_COMMAND + "7001010700000000"; // Select the antenna port
const ANT_PORT_POWER = RFID_COMMAND + "700106072c010000"; // Set the output power for the logical antenna
const USE_CURRENT_PROFILE = RFID_COMMAND + "7001600b01000000"; // Use the current profile
const ANT_CYCLES = RFID_COMMAND + "70010007ffff0000"; // Specify the number of times the enabled logical antenna port should be cycled through in order to complete protocol command execution
const QUERY_CFG = RFID_COMMAND + "7001000920000000";
const INV_SEL = RFID_COMMAND + "7001020901000000";
const INV_ALG_PARM_0 = RFID_COMMAND + "70010309f7005003";
const INV_CFG = RFID_COMMAND + "7001010901000000";

const isNotification = (value: string) => {
  return value.substring(6, 8) === DESTINATION.NOTIFICATION;
};

const isRFID = (value: string) => {
  return value.substring(6, 8) === DESTINATION.RFID;
};

const getPayload = (value: string) => {
  return value.substring(16);
};

const getEventCode = (value: string) => {
  return value.substring(16, 20);
};

const SendCommand = async (command: string, writeCharacteristic?: Characteristic | null) => {
  if (!writeCharacteristic) {
    console.log("no writeCharacteristic");
    return;
  }
  const fullCommand = defaultCommandHeader + command;
  const commandBase64 = hexToBase64(fullCommand);
  await writeCharacteristic.writeWithResponse(commandBase64);
  console.log("Command sent: ", fullCommand);
};

// Take the data listened to from the CS108 and process it. If tag data than store it, if trigger then start/start inventory
export const processCS108Data = async (value: string, writeCharacteristic?: Characteristic | null) => {
  const decodedValue = base64ToHex(value);
  if (isNotification(decodedValue)) {
    const payload = getPayload(decodedValue);
    switch (payload) {
      case NOTIFICATION.TRIGGER_PUSHED:
        console.log("trigger pushed");
        SendCommand(START_INVENTORY, writeCharacteristic);
        break;
      case NOTIFICATION.TRIGGER_RELEASED:
        console.log("trigger released");
        SendCommand(ABORT_INVENTORY, writeCharacteristic);
        break;
    }
  }
  if (isRFID(decodedValue)) {
    const eventCode = getEventCode(decodedValue);
    switch (eventCode) {
      case EVENT_CODE.TAG_READ:
        console.log("tag read:", decodedValue);
        break;
    }
  }
  return {};
};

// Set required parameters when initially connecting the the CS108
export const startRFIDReader = async (writeCharacteristic: Characteristic) => {
  const commandsByBook = [
    TURN_ON_RFID,
    ANT_PORT_SEL,
    ANT_PORT_POWER,
    USE_CURRENT_PROFILE,
    ANT_CYCLES,
    QUERY_CFG,
    INV_SEL,
    INV_ALG_PARM_0,
    INV_CFG,
  ];

  commandsByBook.forEach(async (command) => {
    SendCommand(command, writeCharacteristic);
  });
};