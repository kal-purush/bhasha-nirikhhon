import { Theme } from '@onr/plugin-antd';
import { ThemeConfig } from 'antd';
import { themeConfig as defaultTheme } from '../configs/theme';

interface Props {
  theme?: Theme;
}

export const getAntdThemeConfig = (props: Props) => {
  const { theme = defaultTheme } = props;

  // Default seed is https://github.com/ant-design/ant-design/blob/master/components/theme/themes/seed.ts
  const antdTheme: ThemeConfig = {
    token: {
      colorPrimary: theme.primaryColor,
      colorInfo: theme.infoColor,
      colorSuccess: theme.successColor,
      colorError: theme.errorColor,
      colorWarning: theme.warningColor,
    },
  };

  return antdTheme;
};