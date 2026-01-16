export interface BotState {
  status: string;
  /** How many steps to move when the user presses a manual movement arrow */
  stepSize: number;
  /** Holds coordinates that the user is currently editing, but has not sent */
  axisBuffer: {
    x?: string;
    y?: string;
    z?: string;
  };
  /** Holds settings that the user is currently editing, but has not sent */
  settingsBuffer: {
    movement_max_spd_x?: string;
    movement_max_spd_y?: string;
    movement_max_spd_z?: string;
    movement_steps_acc_dec_x?: string;
    movement_steps_acc_dec_y?: string;
    movement_steps_acc_dec_z?: string;
    movement_timeout_x?: string;
    movement_timeout_y?: string;
    movement_timeout_z?: string;
  };
  hardware: HardwareState;
}

/** Status registers for the bot's status */
export interface HardwareState {
  x?: number;
  y?: number;
  z?: number;
  s?: number;
  busy?: number;
  last?: string;
  param_version?: number;
  movement_timeout_x?: number;
  movement_timeout_y?: number;
  movement_timeout_z?: number;
  movement_invert_endpoints_x?: number;
  movement_invert_endpoints_y?: number;
  movement_invert_endpoints_z?: number;
  movement_invert_motor_x?: number;
  movement_invert_motor_y?: number;
  movement_invert_motor_z?: number;
  movement_steps_acc_dec_x?: number;
  movement_steps_acc_dec_y?: number;
  movement_steps_acc_dec_z?: number;
  movement_home_up_x?: number;
  movement_home_up_y?: number;
  movement_home_up_z?: number;
  movement_min_spd_x?: number;
  movement_min_spd_y?: number;
  movement_min_spd_z?: number;
  movement_max_spd_x?: number;
  movement_max_spd_y?: number;
  movement_max_spd_z?: number;
  pin0?: string;
  pin1?: string;
  pin2?: string;
  pin3?: string;
  pin4?: string;
  pin5?: string;
  pin6?: string;
  pin7?: string;
  pin8?: string;
  pin9?: string;
  pin10?: string;
  pin11?: string;
  pin12?: string;
  pin13?: string;
  last_sync?: string;
  method?: string;
}