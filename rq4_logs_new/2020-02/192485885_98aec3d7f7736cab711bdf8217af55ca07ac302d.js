/**
 * Section names
 */
const CONTROLLER_NODE = "Controller Node";
const CONNECTION_PARAMS = "Connection Params";
const ACTUATOR = "Actuator";
const BLOWER = "Blower";
const DIGITAL_INPUT = "Digital Input";
const DIGITAL_OUTPUT = "Digital Output";
const NTC = "NTC";
const VOLTAGE_MON = "Voltage Mon";
const HCS = "HCS";
const CLIMATE_ZONE = "Climate Zone";
const HVAC_PARAMS = "HVAC Params";
const LOG_ENTRY = "Log Entry";
const FLOW_TABLE = "Flow Table";
const SPEED_TABLE = "Speed Table";

/**
 * Tooltip descriptions
 */
const CONTROLLER_NODE_TOOLTIP = "No puede declararse Nodo1 porque el Nodo1 siempre es el mando. Será entonces Nodo2, Nodo3, etc";
const CONNECTION_PARAMS_TOOLTIP = "Es obligatorio declararlo si se quiere activar la telemetría a la nube. No hace falta rellenar el campo <i>Section Name</i>";
const ACTUATOR_TOOLTIP = "Pueden declararse hasta 8 por cada nodo de Newton y hasta 4 en el caso de Einstein. El número del pin (hb1_chan, hb2_chan) debe estar entre 1 y 16";
const BLOWER_TOOLTIP = "Pueden declararse hasta 4 por cada nodo. El número del pin (hb1_chan, hb2_chan) debe estar entre 1 y 16 en el caso de Newton y entre 1 y 14 en el caso de Einstein";
const DIGITAL_INPUT_TOOLTIP = "Pueden declararse hasta 6 por cada nodo de Newton y hasta 4 en el caso de Einstein. Es necesario declarar el nodo controlador asociado";
const DIGITAL_OUTPUT_TOOLTIP = "Pueden declararse hasta 8 por cada nodo. Es necesario declarar el nodo controlador asociado";
const NTC_TOOLTIP = "Pueden declararse hasta 8 por cada nodo de Newton y hasta 4 en el caso de Einstein. El número del pin (input_channel) debe estar entre 1 y 8 para Newton y entre 1 y 4 para Einstein";
const VOLTAGE_MON_TOOLTIP = "Ha de declararse Nuno y solo uno por cada nodo controlador";
const HCS_TOOLTIP = "";
const CLIMATE_ZONE_TOOLTIP = "";
const HVAC_PARAMS_TOOLTIP = "";
const LOG_ENTRY_TOOLTIP = "";
const FLOW_TABLE_TOOLTIP = "";
const SPEED_TABLE_TOOLTIP = "";

class Section {
  readingParams = [];
  logEntries = [];

  initLogEntries(logSectionName) {
    this.logEntries = this.readingParams.map((readingParam) => {
      return new LogEntry("", logSectionName, "", readingParam, "", "");
    });
  }
}

class ControllerNode {
  constructor(
    name = "",
    node_id = ""
  ) {
    this.name = name;
    this.node_id = node_id;
  }
}

class ConnectionParams {
  constructor(
    hostname = "",
    devid = "",
    username = "",
    typeName = "",
    topic = "",
    port = "",
    keepalive = "",
    apn = "",
    ntp = ""
  ) {
    this.hostname = hostname;
    this.devid = devid;
    this.username = username;
    this.typeName = typeName;
    this.topic = topic;
    this.port = port;
    this.keepalive = keepalive;
    this.apn = apn;
    this.ntp = ntp;
  }
}

class Actuator extends Section {
  readingParams = ["status", "setPoint", "current_position", "avg_meas_current", "peak_meas_current"];

  constructor(
    name = "",
    node_id = "",
    active = "",
    typeName = "",
    functionName = "",
    hb1_chan = "",
    hb2_chan = "",
    servo_chan = "",
    hysteresis = "",
    block_current_ma = "",
    hold_off_start_ms = "",
    reversed = "",
    safe_setpoint = ""
  ) {
    super();
    this.name = name;
    this.node_id = node_id;
    this.active = active;
    this.typeName = typeName;
    this.functionName = functionName;
    this.hb1_chan = hb1_chan;
    this.hb2_chan = hb2_chan;
    this.servo_chan = servo_chan;
    this.hysteresis = hysteresis;
    this.block_current_ma = block_current_ma;
    this.hold_off_start_ms = hold_off_start_ms;
    this.reversed = reversed;
    this.safe_setpoint = safe_setpoint;
    this.initLogEntries(this.name);
  }
}

class Blower extends Section {
  readingParams = ["setPoint"];

  constructor(
    name = "",
    node_id = "",
    active = "",
    drive_mode = "",
    speed_steps = "",
    hb1_chan = "",
    hb2_chan = "",
    hb3_chan = "",
    pwm_chan = "",
    nom_min_current_ma = "",
    nom_max_current_ma = "",
    hcs_sensor_channel = "",
    hvac_heatonly = "",
    hvac_coolonly = "",
    safe_setpoint = ""
  ) {
    super();
    this.name = name;
    this.node_id = node_id;
    this.active = active;
    this.drive_mode = drive_mode;
    this.speed_steps = speed_steps;
    this.hb1_chan = hb1_chan;
    this.hb2_chan = hb2_chan;
    this.hb3_chan = hb3_chan;
    this.pwm_chan = pwm_chan;
    this.nom_min_current_ma = nom_min_current_ma;
    this.nom_max_current_ma = nom_max_current_ma;
    this.hcs_sensor_channel = hcs_sensor_channel;
    this.hvac_heatonly = hvac_heatonly;
    this.hvac_coolonly = hvac_coolonly;
    this.safe_setpoint = safe_setpoint;
    this.initLogEntries(this.name);
  }
}

class DigitalInput extends Section {
  readingParams = ["state"];

  constructor(
    name = "",
    node_id = "",
    active = "",
    pup_pdn = "",
    input_channel = ""
  ) {
    super();
    this.name = name;
    this.node_id = node_id;
    this.active = active;
    this.pup_pdn = pup_pdn;
    this.input_channel = input_channel;
    this.initLogEntries(this.name);
  }
}

class DigitalOutput extends Section {
  readingParams = ["state"];

  constructor(
    name = "",
    node_id = "",
    active = "",
    onstate = "",
    safe_state = "",
    output_channel = ""
  ) {
    super();
    this.name = name;
    this.node_id = node_id;
    this.active = active;
    this.onstate = onstate;
    this.safe_state = safe_state;
    this.output_channel = output_channel;
    this.initLogEntries(this.name);
  }
}

class Ntc extends Section {
  readingParams = ["temp"];

  constructor(
    name = "",
    node_id = "",
    active = "",
    beta = "",
    r25 = "",
    input_channel = ""
  ) {
    super();
    this.name = name;
    this.node_id = node_id;
    this.active = active;
    this.beta = beta;
    this.r25 = r25;
    this.input_channel = input_channel;
    this.initLogEntries(this.name);
  }
}

class VoltageMon extends Section {
  readingParams = ["BatteryVoltage", "Vpot12Voltage", "Vpot34Voltage", "IOvoltage"];

  constructor(
    name = "",
    node_id = "",
    nominalBatteryVoltage = "",
    batteryUnderVoltageLevel = "",
    batteryOverVoltageLevel = ""
  ) {
    super();
    this.name = name;
    this.node_id = node_id;
    this.nominalBatteryVoltage = nominalBatteryVoltage;
    this.batteryUnderVoltageLevel = batteryUnderVoltageLevel;
    this.batteryOverVoltageLevel = batteryOverVoltageLevel;
    this.initLogEntries(this.name);
  }
}

class Hcs extends Section {
  readingParams = ["Reading10mA", "CurrentMinmA", "CurrentMaxmA"];

  constructor(
    name = "",
    node_id = "",
    active = "",
    sensitivitymva = "",
    mvatzero = "",
    input_channel = "",
    ntc_channel = "",
    alarm_temp = ""
  ) {
    super();
    this.name = name;
    this.node_id = node_id;
    this.active = active;
    this.sensitivitymva = sensitivitymva;
    this.mvatzero = mvatzero;
    this.input_channel = input_channel;
    this.ntc_channel = ntc_channel;
    this.alarm_temp = alarm_temp;
    this.initLogEntries(this.name);
  }
}

class ClimateZone extends Section {
  readingParams = ["controlstatus"];

  constructor(
    name = "",
    pa = "",
    p1 = "",
    lt = "",
    ht = "",
    dt = "",
    minblowerspeedacon = "",
    airflow_up_pos = "",
    airflow_middle_pos = "",
    airflow_down_pos = "",
    setpoint = ""
  ) {
    super();
    this.name = name;
    this.pa = pa;
    this.p1 = p1;
    this.lt = lt;
    this.ht = ht;
    this.dt = dt;
    this.minblowerspeedacon = minblowerspeedacon;
    this.airflow_up_pos = airflow_up_pos;
    this.airflow_middle_pos = airflow_middle_pos;
    this.airflow_down_pos = airflow_down_pos;
    this.setpoint = setpoint;
    this.initLogEntries(this.name);
  }
}

class HvacParams {
  readingParams = ["controlstatus"];

  constructor(
    name = "",
    ACcompressor_inhibit_time = "",
    climamode = "",
    recirccycle = "",
    recircdutycycle = ""
  ) {
    this.name = name;
    this.ACcompressor_inhibit_time = ACcompressor_inhibit_time;
    this.climamode = climamode;
    this.recirccycle = recirccycle;
    this.recircdutycycle = recircdutycycle;
  }
}

class LogEntry {
  constructor(
    name = "",
    deviceName = "",
    zoneName = "",
    paramName = "",
    logPeriodicity = "",
    onlyOnChange = ""
  ) {
    this.name = name;
    this.deviceName = deviceName;
    this.zoneName = zoneName;
    this.paramName = paramName;
    this.logPeriodicity = logPeriodicity;
    this.onlyOnChange = onlyOnChange;
  }
}

class FlowTable {
  constructor(
    s1 = "",
    s2 = "",
    s3 = "",
    s4 = "",
    s5 = "",
    s6 = ""
  ) {
    this.s1 = s1;
    this.s2 = s2;
    this.s3 = s3;
    this.s4 = s4;
    this.s5 = s5;
    this.s6 = s6;
  }
}

class SpeedTable {
  constructor(
    s1 = "",
    s2 = "",
    s3 = "",
    s4 = "",
    s5 = "",
    s6 = ""
  ) {
    this.s1 = s1;
    this.s2 = s2;
    this.s3 = s3;
    this.s4 = s4;
    this.s5 = s5;
    this.s6 = s6;
  }
}