/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{ts,tsx}"
  ],
  theme: {
    extend: {
      width: {
        '800': '800px',
        '966':'966px'
      },
      borderRadius:{
        '20':'20px'
      },
      colors:{
        'gray31':'#4F4F4F',
        'very-dark-gray':"#222"
      }

    },
  },
  plugins: [],
}
