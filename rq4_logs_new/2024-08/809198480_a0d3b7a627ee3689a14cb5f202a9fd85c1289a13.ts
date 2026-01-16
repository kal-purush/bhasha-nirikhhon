import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        'custom-background': '#f8f9f7',
        'green-soft': '#96C49C',
        'green-light': '#A8D0B6',
        'green-medium': '#7CA88E',
      },
      backgroundImage: {
        "gradient-radial": "radial-gradient(var(--tw-gradient-stops))",
        "gradient-conic":
          "conic-gradient(from 180deg at 50% 50%, var(--tw-gradient-stops))",
      },
      fontFamily: {
        'sans': [
          'Helvetica Neue',
          'Helvetica',
          'Roboto',
          'Arial',
          'sans-serif',
        ],
        'courier': ['Courier New', 'Courier', 'monospace'],
        'inter': ['Inter', 'sans-serif'],
      },
      keyframes: {
        hoverBefore: {
          '0%': { transform: 'translateX(100%)', height: '4px' },
          '50%': { height: '100%' },
          '100%': { transform: 'translateX(0)', height: '100%' },
        },
        hoverAfter: {
          '0%': { transform: 'translateX(-100%)', height: '4px' },
          '50%': { height: '100%' },
          '100%': { transform: 'translateX(0)', height: '100%' },
        },
      },
      animation: {
        hoverBefore: 'hoverBefore 0.6s linear forwards',
        hoverAfter: 'hoverAfter 0.8s linear forwards',
      },
    },
  },
  plugins: [],
};
export default config;