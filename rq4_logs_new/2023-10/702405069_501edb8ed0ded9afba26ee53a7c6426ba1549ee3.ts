export interface Employee {
  username: string;
  password: string;
  displayName: string;
  avatar?: string;
  preference: EmployeeWithLunch | EmployeeWithoutLunch;
}

export type EmployeeWithLunch = {
  withLunch: true;
  maxBreakInMinutes: 45;
};

export type EmployeeWithoutLunch = {
  withLunch: false;
  maxBreakInMinutes: 75;
};

export interface EmployeeResponse {
  d: D;
}

export interface D {
  __type: string;
  TodayPunches: TodayPunch[];
  TodayStatus: TodayStatus[];
  ErrorCode: string;
  ErrorDescription: string;
}

export interface TodayPunch {
  PT: string;
}

export interface TodayStatus {
  AD: string;
  IT: string;
  OT: string;
  S: string;
  WH: string;
}

export interface EmployeeMerged extends Employee {
  data: EmployeeResponse;
}