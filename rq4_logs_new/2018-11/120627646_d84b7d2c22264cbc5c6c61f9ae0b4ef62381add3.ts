export interface Alert {
  message: string;
  annexMessage: string;
}

export enum AlertTypes {
  Error,
  Warning,
  Info
}

export interface AlertTypeConfig {
  type: number;
  color: string;
  icon: string;
}

export const AlertTypesConfigList: { [ x: number ]: AlertTypeConfig } = {
  [ AlertTypes.Error ]: {
    type: AlertTypes.Error,
    color: 'warn',
    icon: 'error'
  },
  [ AlertTypes.Warning ]: {
    type: AlertTypes.Warning,
    color: 'primary',
    icon: 'warning'
  },
  [ AlertTypes.Info ]: {
    type: AlertTypes.Info,
    color: 'accent',
    icon: 'info'
  }
};

export interface AlertDescription {
  content: Alert;
  type: AlertTypes;
}