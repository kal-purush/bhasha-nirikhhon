/**
Copyright 2015, Christopher Brown <io@henrian.com>, MIT Licensed

Encoding takes us from the raw source to a more obscure format.
Decoding gets us from that format back to the raw source.

In the case of base64, the raw (unencoded) source is an array of bytes, and the
obscured (encoded) format is the base64 string.
*/

export const STANDARD_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';

/**
Convert a Uint8Array or Array of numbers (in which case each element should be
in the range 0-255) and returns a native Javascript string representation of
the bytes in base64.

Mostly from https://gist.github.com/jonleighton/958841, benchmarks at
http://jsperf.com/encoding-xhr-image-data/5

`alphabet` should contain 65 characters, where `alphabet[64]` is the padding
character.
*/
export function encodeBytes(bytes: number[] | Uint8Array,
                            alphabet = STANDARD_ALPHABET): string {
  var bytes_length = bytes.length;
  var byte_remainder = bytes_length % 3;
  var string_length = bytes_length - byte_remainder;
  var padding = alphabet[64];

  var a: number, b: number, c: number, d: number;
  var chunk: number;

  var characters: string[] = [];

  // Main loop deals with bytes in chunks of 3
  for (var i = 0; i < string_length; i += 3) {
    // Combine the three bytes into a single integer
    chunk = (bytes[i] << 16) | (bytes[i + 1] << 8) | bytes[i + 2];
    // Use bitmasks to extract 6-bit segments from the triplet
    a = (chunk & 16515072) >> 18; // 16515072 = (2^6 - 1) << 18
    b = (chunk & 258048)   >> 12; // 258048   = (2^6 - 1) << 12
    c = (chunk & 4032)     >>  6; // 4032     = (2^6 - 1) << 6
    d = chunk & 63;               // 63       = 2^6 - 1
    // Convert the raw binary segments to the appropriate ASCII encoding
    characters.push(alphabet[a], alphabet[b], alphabet[c], alphabet[d]);
  }

  // Deal with the remaining bytes and padding
  if (byte_remainder == 1) {
    chunk = bytes[string_length];
    a = (chunk & 252) >> 2; // 252 = (2^6 - 1) << 2
    // Set the 4 least significant bits to zero
    b = (chunk & 3)   << 4; // 3   = 2^2 - 1
    characters.push(alphabet[a], alphabet[b], padding, padding);
  }
  else if (byte_remainder == 2) {
    chunk = (bytes[string_length] << 8) | bytes[string_length + 1];
    a = (chunk & 64512) >> 10; // 64512 = (2^6 - 1) << 10
    b = (chunk & 1008)  >>  4; // 1008  = (2^6 - 1) << 4
    // Set the 2 least significant bits to zero
    c = (chunk & 15)    <<  2; // 15    = 2^4 - 1
    characters.push(alphabet[a], alphabet[b], alphabet[c], padding);
  }
  // otherwise: no padding required

  return characters.join('');
}

/**
Decode a base64-encoded Javascript string into an array of numbers, all of will
be in the range 0-255 (i.e., byte-sized).

Based on https://github.com/danguer/blog-examples/blob/master/js/base64-binary.js,
which is Copyright 2011, Daniel Guerrero, BSD Licensed.
*/
export function decodeString(str: string,
                             alphabet = STANDARD_ALPHABET): number[] {
  var string_length = str.length;
  var bytes: number[] = [];
  for (var index = 0; index < string_length; index += 4) {
    // get the values of the next 4 base64 chars
    var c1 = alphabet.indexOf(str[index    ]);
    var c2 = alphabet.indexOf(str[index + 1]);
    var c3 = alphabet.indexOf(str[index + 2]);
    var c4 = alphabet.indexOf(str[index + 3]);
    // and derive the original bytes from them
    var b1 = (c1 << 2) | (c2 >> 4);
    var b2 = ((c2 & 15) << 4) | (c3 >> 2);
    var b3 = ((c3 & 3) << 6) | c4;
    // detect padding chars and adjust final length accordingly
    if (c3 === 64) {
      if (c4 === 64) {
        // 2 padding bytes
        bytes.push(b1);
      }
      else {
        // 1 padding byte
        bytes.push(b1, b2);
      }
    }
    else {
      // no padding
      bytes.push(b1, b2, b3);
    }
  }

  return bytes;
}