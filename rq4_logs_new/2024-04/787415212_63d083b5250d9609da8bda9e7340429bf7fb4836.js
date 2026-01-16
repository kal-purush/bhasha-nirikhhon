/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  daisyui: {
    themes: ["forest"],
  },
  theme: { container: {
    center: true,
  },
    extend: {},
  },
  plugins: [require("daisyui")],
}