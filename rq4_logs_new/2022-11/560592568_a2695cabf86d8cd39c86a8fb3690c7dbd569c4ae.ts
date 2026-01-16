export const BASE_CONFIG = {
  App: {
    Version: process.env.REACT_APP_VERSION,
    Name: process.env.REACT_APP_NAME,
  },
} as const;