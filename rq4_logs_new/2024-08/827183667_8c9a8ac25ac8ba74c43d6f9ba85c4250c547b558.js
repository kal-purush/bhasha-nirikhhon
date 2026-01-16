const config = {
  development: {
    apiBaseUrl: "http://localhost:3000",
  },
  production: {
    apiBaseUrl: "",
  },
};

export default config[process.env.NODE_ENV || "development"];