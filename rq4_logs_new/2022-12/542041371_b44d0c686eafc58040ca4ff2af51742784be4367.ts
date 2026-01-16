import { Theme } from 'antd/lib/config-provider/context';

const themes: { [key: string]: Theme } = {
  default: {},
  gray: {
    primaryColor: '#607D8B',
    infoColor: '#607D8B',
    processingColor: '#607D8B',
    successColor: '#00E676',
    errorColor: '#ff4d4f',
    warningColor: '#FFEB3B',
  },
  brown: {
    primaryColor: '#795548',
    infoColor: '#795548',
    processingColor: '#795548',
    successColor: '#00E676',
    errorColor: '#ff4d4f',
    warningColor: '#FFEB3B',
  },
  orange: {
    primaryColor: '#FB8C00',
    infoColor: '#FB8C00',
    processingColor: '#FB8C00',
    successColor: '#00E676',
    errorColor: '#ff4d4f',
    warningColor: '#FFEB3B',
  },
  teal: {
    primaryColor: '#26A69A',
    infoColor: '#26A69A',
    processingColor: '#26A69A',
    successColor: '#00E676',
    errorColor: '#ff4d4f',
    warningColor: '#FFEB3B',
  },
  cyan: {
    primaryColor: '#26C6DA',
    infoColor: '#26C6DA',
    processingColor: '#26C6DA',
    successColor: '#00E676',
    errorColor: '#ff4d4f',
    warningColor: '#FFEB3B',
  },
  blue: {
    primaryColor: '#03A9F4',
    infoColor: '#03A9F4',
    processingColor: '#03A9F4',
    successColor: '#00E676',
    errorColor: '#ff4d4f',
    warningColor: '#FFEB3B',
  },
  purple: {
    primaryColor: '#673AB7',
    infoColor: '#673AB7',
    processingColor: '#673AB7',
    successColor: '#00E676',
    errorColor: '#ff4d4f',
    warningColor: '#FFEB3B',
  },
  pink: {
    primaryColor: '#EC407A',
    infoColor: '#EC407A',
    processingColor: '#EC407A',
    successColor: '#00E676',
    errorColor: '#ff4d4f',
    warningColor: '#FFEB3B',
  },
};

export const themeList = Object.keys(themes).map(theme => ({
  label: theme,
  value: theme,
}));

export default themes;