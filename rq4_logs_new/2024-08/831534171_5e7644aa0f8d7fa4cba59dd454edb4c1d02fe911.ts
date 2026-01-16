import axios from "axios";

const axiosConfig = axios.create({
  baseURL: process.env.NEXT_PUBLIC_BACKEND_URL, // Ensure this is correct
  headers: {
    "Content-Type": "application/json",
  },
});
axiosConfig.interceptors.request.use(
  function (config) {
    const jwt = localStorage.getItem("token");

    console.log("jwt", jwt);
    if (jwt) {
      config.headers.Authorization = "Bearer " + jwt;
    }
    return config;
  },
  function (err) {
    return Promise.reject(err);
  }
);

axiosConfig.interceptors.response.use(
  function (response) {
    return response;
  },
  function (err) {
    // Check if err.response is defined before accessing its properties
    if (err.response) {
      if (err.response.status === 401) {
        // Handle unauthorized access, e.g., by redirecting to the login page
        // localStorage.removeItem("token");
        // window.location.href = "/login";
      }
    } else {
      console.error("Network or other error occurred", err);
      // Handle errors where no response is received (e.g., network issues)
    }
    return Promise.reject(err);
  }
);

export default axiosConfig;