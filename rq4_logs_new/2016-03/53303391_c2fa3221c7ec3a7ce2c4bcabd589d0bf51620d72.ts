/// <reference path="../../typings/main/ambient/jasmine/jasmine.d.ts"/>


class FizzBuzz {
  generate(n: number) {
    if (n % 15 === 0) {
      return 'FIZZ_BUZZ'
    } else if (n % 5 === 0) {
      return 'BUZZ'
    } else if (n % 3 === 0) {
      return 'FIZZ'
    }
    return n;
  }
}

describe('FizzBuzzのgenerateメソッドは',() => {
    beforeEach(() => {
        this.fizzbuzz = new FizzBuzz();
        this.FIZZ = 'FIZZ';
        this.BUZZ = 'BUZZ';
        this.FIZZ_BUZZ = 'FIZZ_BUZZ';
    });

    it('1を渡すと、1が返ってくる',() => {
        var result = this.fizzbuzz.generate(1);
        expect(result).toBe(1);
    });
    it('2を渡すと、2が返ってくる',() => {
        var result = this.fizzbuzz.generate(2);
        expect(result).toBe(2);
    });
    it('3を渡すと、Fizzが返ってくる',() => {
        var result = this.fizzbuzz.generate(3);
        expect(result).toBe(this.FIZZ);
    });
    it('4を渡すと、4が返ってくる',() => {
        var result = this.fizzbuzz.generate(4);
        expect(result).toBe(4);
    });
    it('5を渡すと、Buzzが返ってくる',() => {
        var result = this.fizzbuzz.generate(5);
        expect(result).toBe(this.BUZZ);
    });
    it('6を渡すと、Fizzが返ってくる',() => {
        var result = this.fizzbuzz.generate(6);
        expect(result).toBe(this.FIZZ);
    });
    it('10を渡すと、Buzzが返ってくる',() => {
        var result = this.fizzbuzz.generate(10);
        expect(result).toBe(this.BUZZ);
    });
    it('15を渡すと、FizzBuzzが返ってくる',() => {
        var result = this.fizzbuzz.generate(15);
        expect(result).toBe(this.FIZZ_BUZZ);
    });
    it('30を渡すと、FizzBuzzが返ってくる',() => {
        var result = this.fizzbuzz.generate(30);
        expect(result).toBe(this.FIZZ_BUZZ);
    });
});