// package: relay
// file: meter.proto

import * as jspb from "google-protobuf";

export class RelayState extends jspb.Message {
  getIsOn(): boolean;
  setIsOn(value: boolean): void;

  getCurrentReading(): number;
  setCurrentReading(value: number): void;

  getVoltageReading(): number;
  setVoltageReading(value: number): void;

  getPowerFactor(): number;
  setPowerFactor(value: number): void;

  getTimestamp(): number;
  setTimestamp(value: number): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): RelayState.AsObject;
  static toObject(includeInstance: boolean, msg: RelayState): RelayState.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: RelayState, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): RelayState;
  static deserializeBinaryFromReader(message: RelayState, reader: jspb.BinaryReader): RelayState;
}

export namespace RelayState {
  export type AsObject = {
    isOn: boolean,
    currentReading: number,
    voltageReading: number,
    powerFactor: number,
    timestamp: number,
  }
}
