export type ThemeCommon = {
  font: {
    lineHeight: {
      s: number;
      m: number;
      l: number;
    };
    letterSpacing: {
      s: number;
      m: number;
    };
    weight: {
      r: number;
      m: number;
      b: number;
    };
  };
  radius: {
    none: string;
    s: string;
    m: string;
    l: string;
    full: string;
  };
  elevation: {
    style1: string;
    style2: string;
    style3: string;
    style4: string;
    style5: string;
    style6: string;
    style7: string;
    style8: string;
  };
};

export type ThemeColors = {
  text: {
    body: string;
    description: string;
    placeHolder: string;
    onFill: string;
    link: string;
    hover: string;
    active: string;
    visited: string;
    alert: string;
    disabled: string;
  };
  icon: {
    label: string;
    active: string;
    disabled: string;
    alert: string;
  };
  button: {
    normal: string;
    active: string;
    hover: string;
    disabled: string;
  };
  background: {
    primary: string;
    secondary: string;
    tertiary: string;
  };
  border: {
    field: string;
    divider: string;
    focused: string;
    selected: string;
    alert: string;
    disabled: string;
  };
  status: {
    success: string;
    alert: string;
    warning: string;
  };
  chart: {
    primary: string;
    secondary: string;
  };
};

export type Theme = ThemeCommon & ThemeColors;