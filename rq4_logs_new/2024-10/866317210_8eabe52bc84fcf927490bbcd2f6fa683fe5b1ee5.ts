import type { Config } from 'tailwindcss'
import { type PluginAPI } from 'tailwindcss/types/config'

const config: Config = {
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  darkMode: 'selector',
  theme: {
    extend: {
      typography: (theme: PluginAPI['theme']) => ({
        DEFAULT: {
          css: {
            color: theme('colors.gray.900'),
            maxWidth: '65ch',
            a: {
              color: theme('colors.blue.500'),
              '&:hover': {
                color: theme('colors.blue.700'),
              },
            },
            h1: {
              fontSize: theme('fontSize.3xl'),
              fontWeight: theme('fontWeight.bold'),
              marginTop: theme('spacing.8'),
              marginBottom: theme('spacing.4'),
            },
            h2: {
              fontSize: theme('fontSize.2xl'),
              fontWeight: theme('fontWeight.semibold'),
              marginTop: theme('spacing.6'),
              marginBottom: theme('spacing.3'),
            },
            h3: {
              fontSize: theme('fontSize.xl'),
              fontWeight: theme('fontWeight.semibold'),
              marginTop: theme('spacing.5'),
              marginBottom: theme('spacing.2'),
            },
            h4: {
              fontSize: theme('fontSize.lg'),
              fontWeight: theme('fontWeight.semibold'),
              marginTop: theme('spacing.4'),
              marginBottom: theme('spacing.2'),
            },
            p: {
              marginTop: theme('spacing.4'),
              marginBottom: theme('spacing.4'),
            },
            'ul, ol': {
              paddingLeft: theme('spacing.6'),
            },
            li: {
              marginTop: theme('spacing.2'),
              marginBottom: theme('spacing.2'),
            },
            code: {
              color: theme('colors.pink.500'),
              backgroundColor: theme('colors.gray.100'),
              paddingLeft: '4px',
              paddingRight: '4px',
              paddingTop: '2px',
              paddingBottom: '2px',
              borderRadius: '0.25rem',
              fontSize: '0.875em',
            },
            'code::before': {
              content: 'none',
            },
            'code::after': {
              content: 'none',
            },
            pre: {
              backgroundColor: theme('colors.gray.800'),
              color: theme('colors.gray.100'),
              fontSize: '0.875em',
              lineHeight: '1.7142857',
              marginTop: theme('spacing.4'),
              marginBottom: theme('spacing.4'),
              padding: theme('spacing.4'),
              borderRadius: theme('borderRadius.md'),
            },
          },
        },
        dark: {
          css: {
            color: theme('colors.gray.100'),
            a: {
              color: theme('colors.blue.400'),
              '&:hover': {
                color: theme('colors.blue.300'),
              },
            },
            'h1, h2, h3, h4': {
              color: theme('colors.gray.100'),
            },
            code: {
              color: theme('colors.pink.400'),
              backgroundColor: theme('colors.gray.800'),
            },
            pre: {
              backgroundColor: theme('colors.gray.900'),
              color: theme('colors.gray.200'),
            },
          },
        },
      }),
    },
  },
  plugins: [require('@tailwindcss/typography')],
}

export default config