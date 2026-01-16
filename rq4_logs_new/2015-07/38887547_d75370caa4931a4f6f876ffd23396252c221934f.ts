declare var process, require, module;

interface Format {
  regExp: RegExp;
  generate: (input: string) => string;
}

const DIGITS = '0123456789';
const HEXA_UPPER = '0123456789ABCDEF';
const HEXA_LOWER = '0123456789abcdef';
const ALPHA_UPPER = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
const ALPHA_LOWER = 'abcdefghijklmnopqrstuvwxyz';
const ALPHA_MIXED = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
const ALPHANUMERIC_UPPER = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
const ALPHANUMERIC_LOWER = '0123456789abcdefghijklmnopqrstuvwxyz';
const ALPHANUMERIC_MIXED = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

function sample(population: string, length: number): string {
  var choices: string[] = [];
  for (var i = 0; i < length; i++) {
    choices.push(population[(Math.random() * population.length) | 0]);
  }
  return choices.join('');
}

function randomInteger(min: number, max: number): number {
  return ((Math.random() * (max - min)) + min) | 0;
}

function uuid(pop: string) {
  return `${sample(pop, 8)}-${sample(pop, 4)}-${sample(pop, 4)}-${sample(pop, 4)}-${sample(pop, 12)}`;
}

const predefined_formats: Format[] = [
  {
    // uppercase uuid
    regExp: /^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$/,
    generate: (input) => uuid(HEXA_UPPER)
  },
  {
    // lowercase uuid
    regExp: /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    generate: (input) => uuid(HEXA_LOWER)
  },
  {
    // IPv4
    regExp: /^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/,
    generate: (input) => {
      return `${randomInteger(0, 256)}.${randomInteger(0, 256)}.${randomInteger(0, 256)}.${randomInteger(0, 256)}`;
    }
  },
  {
    // general digits-only
    regExp: /^[0-9]+$/,
    generate: (input) => sample(DIGITS, input.length)
  },
  {
    // general uppercase hexadecimal
    regExp: /^[0-9A-F]+$/,
    generate: (input) => sample(HEXA_UPPER, input.length)
  },
  {
    // general lowercase hexadecimal
    regExp: /^[0-9a-f]+$/,
    generate: (input) => sample(HEXA_LOWER, input.length)
  },
  {
    // general uppercase alphabetic
    regExp: /^[A-Z]+$/,
    generate: (input) => sample(ALPHA_UPPER, input.length)
  },
  {
    // general lowercase alphabetic
    regExp: /^[a-z]+$/,
    generate: (input) => sample(ALPHA_LOWER, input.length)
  },
  {
    // general mixedcase alphabetic
    regExp: /^[A-Za-z]+$/,
    generate: (input) => sample(ALPHA_MIXED, input.length)
  },
  {
    // general uppercase alphanumeric
    regExp: /^[0-9A-Z]+$/,
    generate: (input) => sample(ALPHANUMERIC_UPPER, input.length)
  },
  {
    // general lowercase alphanumeric
    regExp: /^[0-9a-z]+$/,
    generate: (input) => sample(ALPHANUMERIC_LOWER, input.length)
  },
  {
    // general mixedcase alphanumeric
    regExp: /^[0-9A-Za-z]+$/,
    generate: (input) => sample(ALPHANUMERIC_MIXED, input.length)
  },
]

export function convert(input: string): string {
  for (var i = 0, format: Format; (format = predefined_formats[i]); i++) {
    if (format.regExp.test(input)) {
      return format.generate(input);
    }
  }
  throw new Error(`Unable to recognize format of input: ${input}`)
}

export function readStream(stream, callback: (error: Error, data?: string) => void) {
  var chunks = [];
  stream
  .on('error', (error) => {
    callback(error);
  })
  .on('data', (chunk) => {
    chunks.push(chunk);
  })
  .on('end', () => {
    callback(null, chunks.join(''));
  });
}