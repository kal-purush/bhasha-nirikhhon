export default {
  computed: {
    moneyInputInvalid() {
      return this.originalMoney < 1000;
    }
  }
};