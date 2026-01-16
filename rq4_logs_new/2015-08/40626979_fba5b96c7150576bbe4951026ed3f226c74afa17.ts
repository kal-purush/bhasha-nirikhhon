export function zeros (size: number): Array<number> {
  return Array.apply(null, Array(size)).map(Number.prototype.valueOf, 0);
}

export function sum (a1: Array<number>): number {
  return a1.reduce((a, b) => {
    return a + b;
  });
}

export function squareSum (a1: Array<number>): number {
  return a1.reduce((a, b) => {
    return a + Math.pow(b, 2);
  });
}

export function mean (a1: Array<number>): number {
  return sum(a1) / a1.length; 
}

export function rootMeanSquare (a1: Array<number>): number {
  return Math.sqrt(squareSum(a1));
}

export function add (...arrays: number[][]): Array<number> {
  let ret = [],
      size = arrays[0].length;
      
  arrays.forEach((a) => {
    if (size !== a.length) {
      throw new Error('The size of the arrays must be the same');
    }
  });
  
  for (let i = 0; i < arrays[0].length; i++) { 
    ret[i] = 0;  
    for (let j = 0; j < arrays.length; j++) {
      ret[i] += arrays[j][i];     
    }
  }
  
  return ret;
}

export function addScalar (a1: Array<number>, s: number): Array<number> {
  let ret = [];
  
  for (let i = 0; i < a1.length; i++) {
    ret[i] = a1[i] + s;
  }
  
  return ret;
}

export function sub (...arrays: number[][]): Array<number> {
  let ret = [],
      size = arrays[0].length;
      
  arrays.forEach((a) => {
    if (size !== a.length) {
      throw new Error('The size of the arrays must be the same');
    }
  });
  
  for (let i = 0; i < arrays[0].length; i++) { 
    ret[i] = arrays[0][i];  
    for (let j = 1; j < arrays.length; j++) {
      ret[i] -= arrays[j][i];     
    }
  }
  
  return ret;
}

export function multiply (...arrays: number[][]): Array<number> {
  let ret = [],
      size = arrays[0].length;
      
  arrays.forEach((a) => {
    if (size !== a.length) {
      throw new Error('The size of the arrays must be the same');
    }
  });
  
  for (let i = 0; i < arrays[0].length; i++) { 
    ret[i] = 1;  
    for (let j = 0; j < arrays.length; j++) {
      ret[i] *= arrays[j][i];     
    }
  }
  
  return ret;
}

export function multiplyByScalar (a1: Array<number>, scalar: number): Array<number> {
  let ret = [];
  
  for (let i = 0; i < a1.length; i++) {
    ret[i] = a1[i] * scalar;
  }
  
  return ret;
}