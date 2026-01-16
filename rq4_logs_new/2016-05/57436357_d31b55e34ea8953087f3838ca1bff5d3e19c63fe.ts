import 'ts-helpers';

interface PrimeInterface {
  isPrime(n: number): boolean;
}

interface PrimeInterfaceExtended extends PrimeInterface {
  primeFactors(n: number): Array<number>;
}

class PrimeService implements PrimeInterface {
  protected divisor: number;

  constructor(divisor: number = 2) {
    this.divisor = divisor;
  }

  isPrime(n: number): boolean {
    let currentDivisor = this.divisor;

    while (n > currentDivisor) {
      if (n % currentDivisor === 0) {
        return false;
      } else {
        currentDivisor++;
      }
    }

    return true;
  }
}

class PrimeServiceExtended extends PrimeService implements PrimeInterfaceExtended {
  constructor(divisor: number = 2) {
    super(divisor);
  }

  primeFactors(n: number): Array<number> {
    let factors = [];
    let currentDivisor = this.divisor;
    let currentN = n;

    while (currentN > 2) {
      if (currentN % currentDivisor === 0) {
        factors.push(currentDivisor);
        currentN = currentN / currentDivisor;
      } else {
        currentDivisor++;
      }
    }

    return factors;
  }
}

export default PrimeServiceExtended;