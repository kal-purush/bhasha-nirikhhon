/* eslint-disable @typescript-eslint/no-explicit-any */
// import type { Config } from 'tailwindcss';

import { colors } from "./tailwindColors";


const config = {
  darkMode: 'class',
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors,
      screens: {
        'lt-short': { raw: '(max-height: 549px)' },
        short: { raw: '(min-height: 550px)' },
        'lt-tall': { raw: '(max-height: 767px)' },
        tall: { raw: '(min-height: 768px)' },
        'lt-tiny': { raw: '(max-width: 450px)' },
        'lt-sm': { raw: '(max-width: 639px)' },
        'lt-md': { raw: '(max-width: 767px)' },
        'lt-lg': { raw: '(max-width: 1023px)' },
        'not-touch': { raw: '(hover: hover) and (pointer: fine)' },
        'has-touch': { raw: '(hover: none), (pointer: coarse)' },
        'touch-landscape': { raw: '(hover: none) and (pointer: coarse) and (orientation: landscape)' },
      },
      animation: {
        'shake-x': 'shake-x 0.82s cubic-bezier(.36,.07,.19,.97) both',
        'shake-y': 'shake-y 0.82s cubic-bezier(.36,.07,.19,.97) both',
        'slide-up': 'slide-up 0.82s cubic-bezier(.36,.07,.19,.97) both',
        'slide-down': 'slide-down 0.82s cubic-bezier(.36,.07,.19,.97) both',
      },
      keyframes: {
        'shake-x': {
          '10%, 90%': {
            transform: 'translate3d(-1px, 0, 0)',
          },
          '20%, 80%': {
            transform: 'translate3d(2px, 0, 0)',
          },
          '30%, 50%, 70%': {
            transform: 'translate3d(-4px, 0, 0)',
          },
          '40%, 60%': {
            transform: 'translate3d(4px, 0, 0)',
          },
        },
        'shake-y': {
          '10%, 90%': {
            transform: 'translate3d(0, -1px, 0)',
          },
          '20%, 80%': {
            transform: 'translate3d(0, 2px, 0)',
          },
          '30%, 50%, 70%': {
            transform: 'translate3d(0, -3px, 0)',
          },
          '40%, 60%': {
            transform: 'translate3d(0, 3px, 0)',
          },
        },
        'slide-up': {
          '0%': {
            'max-height': '0',
            'margin-bottom': '-1rem',
          },
          '100%': {
            'max-height': '250px',
            'margin-bottom': '0',
          },
        },
        'slide-down': {
          '0%': {
            transform: 'translateY(-100%)',
            opacity: '0',
          },
          '100%': {
            transform: 'translateY(0)',
            opacity: '1',
          },
        },
      },
    },
  },
};
export default config;