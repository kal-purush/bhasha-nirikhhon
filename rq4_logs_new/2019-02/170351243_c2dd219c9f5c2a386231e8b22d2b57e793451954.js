const axios = require("axios");

module.exports = {
  get: (fileId, secretKey) => {
    return axios.get(`https://api.figma.com/v1/files/${fileId}`, {
      headers: {
        "X-Figma-Token": secretKey
      }
    });
  }
};