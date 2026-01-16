/* eslint-disable */
import Long from "long";
import _m0 from "protobufjs/minimal";

export const protobufPackage = "contracts";

export enum SiteState {
  /** SiteState_UNKNOWN - Default value, used as a placeholder */
  SiteState_UNKNOWN = 0,
  /** SiteState_ONLINE - Relay is on */
  SiteState_ONLINE = 1,
  /** SiteState_OFFLINE - Relay is off */
  SiteState_OFFLINE = 2,
  /** SiteState_ERROR - Not available or missing */
  SiteState_ERROR = 3,
  UNRECOGNIZED = -1,
}

export function siteStateFromJSON(object: any): SiteState {
  switch (object) {
    case 0:
    case "SiteState_UNKNOWN":
      return SiteState.SiteState_UNKNOWN;
    case 1:
    case "SiteState_ONLINE":
      return SiteState.SiteState_ONLINE;
    case 2:
    case "SiteState_OFFLINE":
      return SiteState.SiteState_OFFLINE;
    case 3:
    case "SiteState_ERROR":
      return SiteState.SiteState_ERROR;
    case -1:
    case "UNRECOGNIZED":
    default:
      return SiteState.UNRECOGNIZED;
  }
}

export function siteStateToJSON(object: SiteState): string {
  switch (object) {
    case SiteState.SiteState_UNKNOWN:
      return "SiteState_UNKNOWN";
    case SiteState.SiteState_ONLINE:
      return "SiteState_ONLINE";
    case SiteState.SiteState_OFFLINE:
      return "SiteState_OFFLINE";
    case SiteState.SiteState_ERROR:
      return "SiteState_ERROR";
    case SiteState.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export enum PlugState {
  /** PlugState_UNKNOWN - Default value, used as a placeholder */
  PlugState_UNKNOWN = 0,
  /** PlugState_ON - Relay is on */
  PlugState_ON = 1,
  /** PlugState_OFF - Relay is off */
  PlugState_OFF = 2,
  /** PlugState_MIA - Not available or missing */
  PlugState_MIA = 3,
  /** PlugState_OVERCURRENT - Overcurrent detected */
  PlugState_OVERCURRENT = 4,
  UNRECOGNIZED = -1,
}

export function plugStateFromJSON(object: any): PlugState {
  switch (object) {
    case 0:
    case "PlugState_UNKNOWN":
      return PlugState.PlugState_UNKNOWN;
    case 1:
    case "PlugState_ON":
      return PlugState.PlugState_ON;
    case 2:
    case "PlugState_OFF":
      return PlugState.PlugState_OFF;
    case 3:
    case "PlugState_MIA":
      return PlugState.PlugState_MIA;
    case 4:
    case "PlugState_OVERCURRENT":
      return PlugState.PlugState_OVERCURRENT;
    case -1:
    case "UNRECOGNIZED":
    default:
      return PlugState.UNRECOGNIZED;
  }
}

export function plugStateToJSON(object: PlugState): string {
  switch (object) {
    case PlugState.PlugState_UNKNOWN:
      return "PlugState_UNKNOWN";
    case PlugState.PlugState_ON:
      return "PlugState_ON";
    case PlugState.PlugState_OFF:
      return "PlugState_OFF";
    case PlugState.PlugState_MIA:
      return "PlugState_MIA";
    case PlugState.PlugState_OVERCURRENT:
      return "PlugState_OVERCURRENT";
    case PlugState.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface Plug {
  /** The plug ID */
  plug_id: string;
  /** The site ID */
  site_id: string;
  /** The readings for the plug */
  readings: Reading[];
}

export interface PlugSettings {
  /** The name of the plug */
  name: string;
  /** The site ID */
  site_id: string;
  /** The plug ID */
  plug_id: string;
  /** The strategy for the plug */
  strategy: PlugStrategy | undefined;
}

export interface PlugStrategy {
  /** Whether the plug requires the user to turn it on */
  always_on: boolean;
  /** People sign up to be owners if there's a strategy that requires it */
  owner_ids: string[];
  /** The duration the user is signing up to pay for the plug for */
  duration_ms: number;
}

export interface Site {
  /** The site ID */
  site_id: string;
  /** The site name */
  site_name: string;
  /** The state of the site */
  state: SiteState;
}

export interface SiteSetting {
  /** The name of the site */
  name: string;
  /** The site ID */
  site_id: string;
  /** People who have admin control over the site */
  owner_ids: string[];
  /** The strategy for the site */
  strategy: SiteStrategy | undefined;
}

/** may need to capture the strategy for the site as a whole */
export interface SiteStrategy {
}

export interface Reading {
  /** Indicates if the relay is currently on, off, MIA, etc. */
  state: PlugState;
  /** The current power reading in watts */
  current: number;
  /** The voltage reading in volts */
  voltage: number;
  /** Power factor, typically a value between -1 and 1 */
  power_factor: number;
  /** Timestamp of the reading in seconds since epoch */
  timestamp: number;
}

function createBasePlug(): Plug {
  return { plug_id: "", site_id: "", readings: [] };
}

export const Plug = {
  encode(message: Plug, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.plug_id !== "") {
      writer.uint32(10).string(message.plug_id);
    }
    if (message.site_id !== "") {
      writer.uint32(18).string(message.site_id);
    }
    for (const v of message.readings) {
      Reading.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Plug {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePlug();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.plug_id = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.site_id = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.readings.push(Reading.decode(reader, reader.uint32()));
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): Plug {
    return {
      plug_id: isSet(object.plug_id) ? globalThis.String(object.plug_id) : "",
      site_id: isSet(object.site_id) ? globalThis.String(object.site_id) : "",
      readings: globalThis.Array.isArray(object?.readings) ? object.readings.map((e: any) => Reading.fromJSON(e)) : [],
    };
  },

  toJSON(message: Plug): unknown {
    const obj: any = {};
    if (message.plug_id !== "") {
      obj.plug_id = message.plug_id;
    }
    if (message.site_id !== "") {
      obj.site_id = message.site_id;
    }
    if (message.readings?.length) {
      obj.readings = message.readings.map((e) => Reading.toJSON(e));
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<Plug>, I>>(base?: I): Plug {
    return Plug.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<Plug>, I>>(object: I): Plug {
    const message = createBasePlug();
    message.plug_id = object.plug_id ?? "";
    message.site_id = object.site_id ?? "";
    message.readings = object.readings?.map((e) => Reading.fromPartial(e)) || [];
    return message;
  },
};

function createBasePlugSettings(): PlugSettings {
  return { name: "", site_id: "", plug_id: "", strategy: undefined };
}

export const PlugSettings = {
  encode(message: PlugSettings, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.name !== "") {
      writer.uint32(10).string(message.name);
    }
    if (message.site_id !== "") {
      writer.uint32(18).string(message.site_id);
    }
    if (message.plug_id !== "") {
      writer.uint32(26).string(message.plug_id);
    }
    if (message.strategy !== undefined) {
      PlugStrategy.encode(message.strategy, writer.uint32(42).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PlugSettings {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePlugSettings();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.name = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.site_id = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.plug_id = reader.string();
          continue;
        case 5:
          if (tag !== 42) {
            break;
          }

          message.strategy = PlugStrategy.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PlugSettings {
    return {
      name: isSet(object.name) ? globalThis.String(object.name) : "",
      site_id: isSet(object.site_id) ? globalThis.String(object.site_id) : "",
      plug_id: isSet(object.plug_id) ? globalThis.String(object.plug_id) : "",
      strategy: isSet(object.strategy) ? PlugStrategy.fromJSON(object.strategy) : undefined,
    };
  },

  toJSON(message: PlugSettings): unknown {
    const obj: any = {};
    if (message.name !== "") {
      obj.name = message.name;
    }
    if (message.site_id !== "") {
      obj.site_id = message.site_id;
    }
    if (message.plug_id !== "") {
      obj.plug_id = message.plug_id;
    }
    if (message.strategy !== undefined) {
      obj.strategy = PlugStrategy.toJSON(message.strategy);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PlugSettings>, I>>(base?: I): PlugSettings {
    return PlugSettings.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PlugSettings>, I>>(object: I): PlugSettings {
    const message = createBasePlugSettings();
    message.name = object.name ?? "";
    message.site_id = object.site_id ?? "";
    message.plug_id = object.plug_id ?? "";
    message.strategy = (object.strategy !== undefined && object.strategy !== null)
      ? PlugStrategy.fromPartial(object.strategy)
      : undefined;
    return message;
  },
};

function createBasePlugStrategy(): PlugStrategy {
  return { always_on: false, owner_ids: [], duration_ms: 0 };
}

export const PlugStrategy = {
  encode(message: PlugStrategy, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.always_on === true) {
      writer.uint32(8).bool(message.always_on);
    }
    for (const v of message.owner_ids) {
      writer.uint32(18).string(v!);
    }
    if (message.duration_ms !== 0) {
      writer.uint32(24).int64(message.duration_ms);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PlugStrategy {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePlugStrategy();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.always_on = reader.bool();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.owner_ids.push(reader.string());
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.duration_ms = longToNumber(reader.int64() as Long);
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): PlugStrategy {
    return {
      always_on: isSet(object.always_on) ? globalThis.Boolean(object.always_on) : false,
      owner_ids: globalThis.Array.isArray(object?.owner_ids)
        ? object.owner_ids.map((e: any) => globalThis.String(e))
        : [],
      duration_ms: isSet(object.duration_ms) ? globalThis.Number(object.duration_ms) : 0,
    };
  },

  toJSON(message: PlugStrategy): unknown {
    const obj: any = {};
    if (message.always_on === true) {
      obj.always_on = message.always_on;
    }
    if (message.owner_ids?.length) {
      obj.owner_ids = message.owner_ids;
    }
    if (message.duration_ms !== 0) {
      obj.duration_ms = Math.round(message.duration_ms);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<PlugStrategy>, I>>(base?: I): PlugStrategy {
    return PlugStrategy.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<PlugStrategy>, I>>(object: I): PlugStrategy {
    const message = createBasePlugStrategy();
    message.always_on = object.always_on ?? false;
    message.owner_ids = object.owner_ids?.map((e) => e) || [];
    message.duration_ms = object.duration_ms ?? 0;
    return message;
  },
};

function createBaseSite(): Site {
  return { site_id: "", site_name: "", state: 0 };
}

export const Site = {
  encode(message: Site, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.site_id !== "") {
      writer.uint32(10).string(message.site_id);
    }
    if (message.site_name !== "") {
      writer.uint32(18).string(message.site_name);
    }
    if (message.state !== 0) {
      writer.uint32(24).int32(message.state);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Site {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSite();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.site_id = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.site_name = reader.string();
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.state = reader.int32() as any;
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): Site {
    return {
      site_id: isSet(object.site_id) ? globalThis.String(object.site_id) : "",
      site_name: isSet(object.site_name) ? globalThis.String(object.site_name) : "",
      state: isSet(object.state) ? siteStateFromJSON(object.state) : 0,
    };
  },

  toJSON(message: Site): unknown {
    const obj: any = {};
    if (message.site_id !== "") {
      obj.site_id = message.site_id;
    }
    if (message.site_name !== "") {
      obj.site_name = message.site_name;
    }
    if (message.state !== 0) {
      obj.state = siteStateToJSON(message.state);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<Site>, I>>(base?: I): Site {
    return Site.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<Site>, I>>(object: I): Site {
    const message = createBaseSite();
    message.site_id = object.site_id ?? "";
    message.site_name = object.site_name ?? "";
    message.state = object.state ?? 0;
    return message;
  },
};

function createBaseSiteSetting(): SiteSetting {
  return { name: "", site_id: "", owner_ids: [], strategy: undefined };
}

export const SiteSetting = {
  encode(message: SiteSetting, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.name !== "") {
      writer.uint32(10).string(message.name);
    }
    if (message.site_id !== "") {
      writer.uint32(18).string(message.site_id);
    }
    for (const v of message.owner_ids) {
      writer.uint32(26).string(v!);
    }
    if (message.strategy !== undefined) {
      SiteStrategy.encode(message.strategy, writer.uint32(34).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SiteSetting {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSiteSetting();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.name = reader.string();
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.site_id = reader.string();
          continue;
        case 3:
          if (tag !== 26) {
            break;
          }

          message.owner_ids.push(reader.string());
          continue;
        case 4:
          if (tag !== 34) {
            break;
          }

          message.strategy = SiteStrategy.decode(reader, reader.uint32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): SiteSetting {
    return {
      name: isSet(object.name) ? globalThis.String(object.name) : "",
      site_id: isSet(object.site_id) ? globalThis.String(object.site_id) : "",
      owner_ids: globalThis.Array.isArray(object?.owner_ids)
        ? object.owner_ids.map((e: any) => globalThis.String(e))
        : [],
      strategy: isSet(object.strategy) ? SiteStrategy.fromJSON(object.strategy) : undefined,
    };
  },

  toJSON(message: SiteSetting): unknown {
    const obj: any = {};
    if (message.name !== "") {
      obj.name = message.name;
    }
    if (message.site_id !== "") {
      obj.site_id = message.site_id;
    }
    if (message.owner_ids?.length) {
      obj.owner_ids = message.owner_ids;
    }
    if (message.strategy !== undefined) {
      obj.strategy = SiteStrategy.toJSON(message.strategy);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<SiteSetting>, I>>(base?: I): SiteSetting {
    return SiteSetting.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SiteSetting>, I>>(object: I): SiteSetting {
    const message = createBaseSiteSetting();
    message.name = object.name ?? "";
    message.site_id = object.site_id ?? "";
    message.owner_ids = object.owner_ids?.map((e) => e) || [];
    message.strategy = (object.strategy !== undefined && object.strategy !== null)
      ? SiteStrategy.fromPartial(object.strategy)
      : undefined;
    return message;
  },
};

function createBaseSiteStrategy(): SiteStrategy {
  return {};
}

export const SiteStrategy = {
  encode(_: SiteStrategy, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SiteStrategy {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSiteStrategy();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(_: any): SiteStrategy {
    return {};
  },

  toJSON(_: SiteStrategy): unknown {
    const obj: any = {};
    return obj;
  },

  create<I extends Exact<DeepPartial<SiteStrategy>, I>>(base?: I): SiteStrategy {
    return SiteStrategy.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<SiteStrategy>, I>>(_: I): SiteStrategy {
    const message = createBaseSiteStrategy();
    return message;
  },
};

function createBaseReading(): Reading {
  return { state: 0, current: 0, voltage: 0, power_factor: 0, timestamp: 0 };
}

export const Reading = {
  encode(message: Reading, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.state !== 0) {
      writer.uint32(8).int32(message.state);
    }
    if (message.current !== 0) {
      writer.uint32(17).double(message.current);
    }
    if (message.voltage !== 0) {
      writer.uint32(25).double(message.voltage);
    }
    if (message.power_factor !== 0) {
      writer.uint32(33).double(message.power_factor);
    }
    if (message.timestamp !== 0) {
      writer.uint32(40).int64(message.timestamp);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Reading {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseReading();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 8) {
            break;
          }

          message.state = reader.int32() as any;
          continue;
        case 2:
          if (tag !== 17) {
            break;
          }

          message.current = reader.double();
          continue;
        case 3:
          if (tag !== 25) {
            break;
          }

          message.voltage = reader.double();
          continue;
        case 4:
          if (tag !== 33) {
            break;
          }

          message.power_factor = reader.double();
          continue;
        case 5:
          if (tag !== 40) {
            break;
          }

          message.timestamp = longToNumber(reader.int64() as Long);
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  fromJSON(object: any): Reading {
    return {
      state: isSet(object.state) ? plugStateFromJSON(object.state) : 0,
      current: isSet(object.current) ? globalThis.Number(object.current) : 0,
      voltage: isSet(object.voltage) ? globalThis.Number(object.voltage) : 0,
      power_factor: isSet(object.power_factor) ? globalThis.Number(object.power_factor) : 0,
      timestamp: isSet(object.timestamp) ? globalThis.Number(object.timestamp) : 0,
    };
  },

  toJSON(message: Reading): unknown {
    const obj: any = {};
    if (message.state !== 0) {
      obj.state = plugStateToJSON(message.state);
    }
    if (message.current !== 0) {
      obj.current = message.current;
    }
    if (message.voltage !== 0) {
      obj.voltage = message.voltage;
    }
    if (message.power_factor !== 0) {
      obj.power_factor = message.power_factor;
    }
    if (message.timestamp !== 0) {
      obj.timestamp = Math.round(message.timestamp);
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<Reading>, I>>(base?: I): Reading {
    return Reading.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<Reading>, I>>(object: I): Reading {
    const message = createBaseReading();
    message.state = object.state ?? 0;
    message.current = object.current ?? 0;
    message.voltage = object.voltage ?? 0;
    message.power_factor = object.power_factor ?? 0;
    message.timestamp = object.timestamp ?? 0;
    return message;
  },
};

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

export type DeepPartial<T> = T extends Builtin ? T
  : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>>
  : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };

function longToNumber(long: Long): number {
  if (long.gt(globalThis.Number.MAX_SAFE_INTEGER)) {
    throw new globalThis.Error("Value is larger than Number.MAX_SAFE_INTEGER");
  }
  return long.toNumber();
}

if (_m0.util.Long !== Long) {
  _m0.util.Long = Long as any;
  _m0.configure();
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}