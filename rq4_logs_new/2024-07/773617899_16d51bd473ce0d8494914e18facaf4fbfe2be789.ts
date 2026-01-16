import type { Config } from 'tailwindcss'
import colors from 'tailwindcss/colors'
import Theme from 'tailwindcss/defaultTheme'

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        one: colors.zinc,
        two: colors.slate,
        three: colors.stone
      },
      borderRadius: {
        one: Theme.borderRadius.none
      },
    },
  },
  plugins: [],
};

export default config;