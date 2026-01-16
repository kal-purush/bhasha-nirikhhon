interface theme {
  name: string;
  colors: {
    mainColor: string;
    inactiveColor: string;
    appBackground: string;
    textColor: string;
  };
}

export const light: theme = {
  name: "light-theme",
  colors: {
    mainColor: "#ff1282",
    inactiveColor: "#c4c4c4",
    appBackground: "white",
    textColor: "white",
  },
};

export const dark: theme = {
  name: "light-theme",
  colors: {
    mainColor: "#00FFF7",
    inactiveColor: "#c4c4c4",
    appBackground: "#363442",
    textColor: "#363442",
  },
};