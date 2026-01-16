/** @type {import('tailwindcss').Config} */
exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    screens: {
      'xs': {'max': '639px'},      // Only for mobile (0-639px)
      'sm': {'min': '640px'},      // Small and larger (640px+)
      'md': {'min': '768px'},      // Medium and larger (768px+)
      'lg': {'min': '1024px'},     // Large and larger (1024px+)
      'xl': {'min': '1280px'},     // Extra large and larger (1280px+)
      '2xl': {'min': '1536px'},    // 2XL screens (1536px+)
    },
    extend: {},
  },
  plugins: [],
}