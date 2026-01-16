// src/theme.js
import colors from 'vuetify/es5/util/colors'

console.log('red', colors.red)
export default {
  theme: {
    primary: colors.blue.darken2, // #1976D2
    secondary: colors.grey.darken3, // #424242
    accent: colors.indigo.accent1, // #82B1FF
    error: colors.red.accent2, // #FF5252
    info: colors.blue.base, // #2196F3
    success: colors.green.base, // #4CAF50
    warning: colors.amber.base // #FF5252
  },
  options: {
    customProperties: true
  }
}