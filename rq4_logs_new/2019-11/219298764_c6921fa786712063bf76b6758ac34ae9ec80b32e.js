
export default {
  methods: {
    tabClick (t) {
      this.$router.push({ name: t.name })
    },
    tabRemove (name) {
      this.$store.dispatch(this.GlobalVars.removeTabsMethodName, name)
    }
  },
  mounted () {
    this.$store.dispatch(this.GlobalVars.computeWindowSizeMethodName)
    window.onresize = () => {
      this.$store.dispatch(this.GlobalVars.computeWindowSizeMethodName)
    }
  }
}