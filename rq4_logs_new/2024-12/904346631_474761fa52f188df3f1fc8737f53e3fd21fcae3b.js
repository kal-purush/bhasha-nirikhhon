/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{html,js}"],
  theme: {
    extend: { fontFamily: {
      heading: ['Bebas Neue', 'sans-serif'],
      body: ['Roboto', 'sans-serif'],
    },
    
    },
  },
  plugins: [],
}