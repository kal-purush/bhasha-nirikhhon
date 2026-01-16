const { createApp } = Vue;

createApp({
  data() {
    return {
      message: "Hello Vue!",
    };
  },

  mounted() {
    axios.get("").then((response) => {});
  },
}).mount("#app");