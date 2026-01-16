declare module "coders" {
    module base64 {
        /**
        Copyright 2015, Christopher Brown <io@henrian.com>, MIT Licensed
        
        Encoding takes us from the raw source to a more obscure format.
        Decoding gets us from that format back to the raw source.
        
        In the case of base64, the raw (unencoded) source is an array of bytes, and the
        obscured (encoded) format is the base64 string.
        */
        const STANDARD_ALPHABET: string;
        /**
        Convert a Uint8Array or Array of numbers (in which case each element should be
        in the range 0-255) and returns a native Javascript string representation of
        the bytes in base64.
        
        Mostly from https://gist.github.com/jonleighton/958841, benchmarks at
        http://jsperf.com/encoding-xhr-image-data/5
        
        `alphabet` should contain 65 characters, where `alphabet[64]` is the padding
        character.
        */
        function encodeBytes(bytes: number[] | Uint8Array, alphabet?: string): string;
        /**
        Decode a base64-encoded Javascript string into an array of numbers, all of will
        be in the range 0-255 (i.e., byte-sized).
        
        Based on https://github.com/danguer/blog-examples/blob/master/js/base64-binary.js,
        which is Copyright 2011, Daniel Guerrero, BSD Licensed.
        */
        function decodeString(str: string, alphabet?: string): number[];
    }
}