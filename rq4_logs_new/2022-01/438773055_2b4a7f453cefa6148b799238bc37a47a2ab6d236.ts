export const IN_PROD = process.env.ENV_CLIENT === 'production'
console.log('IN_PROD', IN_PROD)
console.log('first', process.env.ENV_CLIENT)

export const API_URL = IN_PROD
  ? 'http://localhost:8080'
  : 'https://api.alexcode.ninja'