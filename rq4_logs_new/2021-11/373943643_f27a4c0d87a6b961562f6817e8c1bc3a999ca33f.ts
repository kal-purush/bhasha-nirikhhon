import { ActionTree, MutationTree } from "vuex";

enum themes {
  light = "light",
  dark = "dark",
}

enum themeVariables {
  primaryBg = "primary-bg",
  secondaryBg = "secondary-bg",
  text = "text",
  hColor = "headline-color",
  selector = "selector",
  ts = "ts",
  gradientPrimary = "gradient-primary",
  gradientSecondary = "gradient-secondary",
}

type ThemeColorInfo = {
  name: themeVariables;
  value: Map<themes, string>;
};

type ThemeModuleState = {
  activeTheme: themes;
  colors: ThemeColorInfo[];
};

const themeModuleState: ThemeModuleState = {
  activeTheme: themes.light, //TODO: add colors to supereelipses and images

  colors: [
    {
      name: themeVariables.primaryBg,
      value: new Map([
        [themes.light, "#ebebeb"],
        [themes.dark, "#141414"],
      ]),
    },
    {
      name: themeVariables.secondaryBg,
      value: new Map([
        [themes.light, "#ffffff"],
        [themes.dark, "#282828"],
      ]),
    },
    {
      name: themeVariables.text,
      value: new Map([
        [themes.light, "#282828"],
        [themes.dark, "#e6e6e6"],
      ]),
    },
    {
      name: themeVariables.hColor,
      value: new Map([
        [themes.light, "#000000"],
        [themes.dark, "#ffffff"],
      ]),
    },
    {
      name: themeVariables.selector,
      value: new Map([
        [themes.light, "#9d9d3f"],
        [themes.dark, "#e1e199"],
      ]),
    },
    {
      name: themeVariables.ts,
      value: new Map([
        [themes.light, "#ffffff"],
        [themes.dark, "#1f1f1f"],
      ]),
    },
    {
      name: themeVariables.gradientPrimary,
      value: new Map([
        [themes.light, "#B030AC"],
        [themes.dark, "#F9F9E0"],
      ]),
    },
    {
      name: themeVariables.gradientSecondary,
      value: new Map([
        [themes.light, "#409BB0"],
        [themes.dark, "#E6E6E6"],
      ]),
    },
  ],
};

const themeModuleMutations = <MutationTree<ThemeModuleState>>{
  change(state, props: { newTheme: themes }) {
    state.activeTheme = props.newTheme;
  },
};

const themeModuleActions = <ActionTree<ThemeModuleState, null>>{
  init(context) {
    if (window.matchMedia("(prefers-color-scheme)").media !== "not all") {
      if (window.matchMedia("(prefers-color-scheme: dark)").matches) {
        context.dispatch("setDark");
      }
    } else {
      context.dispatch("setLight");
    }
  },

  setLight(context) {
    context.commit("change", { newTheme: themes.light });
    context.dispatch("updateProperties");
  },

  setDark(context) {
    context.commit("change", { newTheme: themes.dark });
    context.dispatch("updateProperties");
  },

  toggle(context) {
    const newTheme =
      context.state.activeTheme === themes.light ? themes.dark : themes.light;

    context.commit("change", { newTheme });
    context.dispatch("updateProperties");
  },

  updateProperties(context) {
    const theme = context.state.activeTheme;
    const colors = context.state.colors;
    const root = document.documentElement.style;

    for (let token = 0; token < colors.length; token += 1) {
      root.setProperty(
        `--${colors[token].name}`,
        `${colors[token].value.get(theme)}`
      );
    }
  },
};

const themeModule = {
  namespaced: true,

  state: themeModuleState,
  mutations: themeModuleMutations,
  actions: themeModuleActions,
};

export { themeVariables, themeModule };