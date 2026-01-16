const { createApp } = Vue

const app = createApp({
  data() {
    return {
      tasks: [
        {
          text: "Attività n.1",
          done: false
        },
        {
          text: "Attività n.2",
          done: true
        },
        {
          text: "Attività n.3",
          done: false
        },
        {
          text: "Attività n.4",
          done: false
        },
        {
          text: "Attività n.5",
          done: true
        },
        {
          text: "Attività n.6",
          done: true
        }
      ],
    }
  },

  methods: {
 

  }
});

app.mount('#app');