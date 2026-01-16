
export const EMPTY_STRING = '';


export class UTF8 {


	public static getBytes(str: string): Uint8Array {
	  str = str.replace(/\r\n/g, '\n');
	  var out = [], p = 0;
	  for (var i = 0; i < str.length; i++) {
	    var c = str.charCodeAt(i);
	    if (c < 128) {
	      out[p++] = c;
	    } else if (c < 2048) {
	      out[p++] = (c >> 6) | 192;
	      out[p++] = (c & 63) | 128;
	    } else {
	      out[p++] = (c >> 12) | 224;
	      out[p++] = ((c >> 6) & 63) | 128;
	      out[p++] = (c & 63) | 128;
	    }
	  }
	  return new Uint8Array(out);
	}


}