export default {
  async mounted() {
    // Get the error from Nuxt $fetchState, which handles both
    // a server-side fetch() error passed to client, or a client-side
    // fetch() error
    // https://nuxtjs.org/api/pages-fetch/#when-to-use-fetch
    const err = await this.$fetchState.error;
    if (err) {
      this.showToastError(`fetch() error: ${err.message}`);
    }
  },
  methods: {
    showToastError(str) {
      this.$toasted.error(str, {
        theme: 'outline',
        position: 'top-center',
        duration: 20000,
        fullWidth: true,
        action: {
          text: 'CLOSE',
          onClick: (e, toastObject) => {
            toastObject.goAway(0);
          }
        }
      });
    }
  }
};