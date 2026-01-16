// - 型定義 =============================================================================================================
import { StylesConfig, ThemeConfig } from "react-select";


export const selectCustomTheme: ThemeConfig = (theme) => ({
  ...theme,
  borderRadius: 5,
  colors: {
    ...theme.colors,
    primary25: "#F1F1F1",
    primary: "#F1F1F1",
  }
})

export const selectCustomStyles: StylesConfig = {
  option: (provided) => ({
    ...provided,
    color: "black",
    padding: 10
  })
}