// input: Array[64]
// output: String
function chrToHexString(chrArray) {
  let hexout = '';
  const twobits = chrArray.map(x => x.toString(2).padStart(2,'0'));

  // js doesn't have 64-bit precision so we have to split it like this.
  // we also don't want to get negative numbers because we don't have uint
  for(let i = 0; i < 64; i+= 8) {
    const piece = twobits.slice(i, i + 8).join('');
    const hex = parseInt(piece, 2).toString(16).padStart(4,'0');
    hexout += hex;
  }

  return hexout;
}

export function Serialize(rawchr, colors) {
  const chr = rawchr.map(chrToHexString);
  const obj = { chr, colors };
  return JSON.stringify(obj);
}

export function Deserialize() {

}