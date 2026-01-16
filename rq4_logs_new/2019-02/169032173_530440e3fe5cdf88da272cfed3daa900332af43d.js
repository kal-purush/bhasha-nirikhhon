class Binary {
    constructor(input) {
      this.input = input;
    }
    Binary() {
       if (this.input.match(/^[0-9]+$/) != null)
       return parseInt(this.input, 10);
       else return 0;

    };
    toDecimal() {
        if (this.input.match(/^[0-1]+$/) != null)
       return parseInt(this.input, 2);
       else return 0;
    }
};
  module.exports = Binary;