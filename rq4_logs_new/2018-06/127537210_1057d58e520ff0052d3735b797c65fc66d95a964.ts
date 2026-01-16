import * as Encoding from "encoding-japanese";
import * as assert from "power-assert";
import { ShioriEncodeLayer } from "../lib/shiori-encode-layer";

const crlf = "\x0d\x0a";

const charsets: {[name: string]: Encoding.Encoding} = {
    "CP932": "SJIS",
    "cp932": "SJIS",
    "SHIFT_JIS": "SJIS",
    "Shift_JIS": "SJIS",
    "shift_jis": "SJIS",
    "UTF-8": "UTF8",
    "utf-8": "UTF8",
};

const headers = [
    "Charset",
    "charset",
    "CHARSET",
    "charSet",
];

const values = [
    "能勢電鉄の表現",
    "あ",
    "Charset: ",
];

const hasCharsetSamples: Array<(header: string, charset: string, value: string) => string> = [
    (header, charset) => `GET SHIORI/3.0${crlf}${header}: ${charset}${crlf}${crlf}`,
    (header, charset, value) => `GET SHIORI/3.0${crlf}Value: ${value}${crlf}${header}: ${charset}${crlf}${crlf}`,
    (header, charset) => `GET SHIORI/3.0${crlf}Value: ${header}: ${crlf}Charset: ${charset}${crlf}${crlf}`,
];

const noCharsetSample = (value: string) => `GET SHIORI/3.0${crlf}Value: ${value}${crlf}${crlf}`;

function convertSample(str: string, charset: Encoding.Encoding, isArraybuffer: boolean) {
    if (isArraybuffer) {
        return new Uint8Array(
            Encoding.convert(str, {to: charset, from: "UNICODE", type: "array"}) as number[],
        ).buffer as ArrayBuffer;
    } else {
        return Encoding.convert(str, {to: charset, from: "UNICODE", type: "string"}) as string;
    }
}

interface Sample<T> {
    charset: Encoding.Encoding;
    content: T;
}

function allHasCharsetSampleStrings(charset: string) {
    const samples = [];
    for (const header of headers) {
        for (const value of values) {
            for (const sample of hasCharsetSamples) {
                samples.push(sample(header, charset, value));
            }
        }
    }
    return samples;
}

function allHasCharsetSamples(): Array<Sample<string>>;
function allHasCharsetSamples(isArraybuffer: true): Array<Sample<ArrayBuffer>>;
function allHasCharsetSamples(isArraybuffer?: true) {
    const samples: Array<Sample<string | ArrayBuffer>> = [];
    for (const charsetKey of Object.keys(charsets)) {
        const charset = charsets[charsetKey];
        samples.push(
            ...allHasCharsetSampleStrings(charsetKey).map(
                (str) => ({ charset, content: convertSample(str, charset, isArraybuffer) }),
            ),
        );
    }
    return samples;
}

function allNoCharsetSamples(): Array<Sample<string>>;
function allNoCharsetSamples(isArraybuffer: true): Array<Sample<ArrayBuffer>>;
function allNoCharsetSamples(isArraybuffer?: true) {
    const samples: Array<Sample<string | ArrayBuffer>> = [];
    for (const charsetKey of Object.keys(charsets)) {
        const charset = charsets[charsetKey];
        for (const value of values) {
            samples.push({ charset: "AUTO", content: convertSample(noCharsetSample(value), charset, isArraybuffer) });
        }
    }
    return samples;
}

describe("ShioriEncodeLayer", () => {
    describe("getStringCharset()", () => {
        it("can detect string", () => {
            for (const sample of allHasCharsetSamples()) {
                assert(sample.charset === ShioriEncodeLayer.getStringCharset(sample.content));
            }
        });

        it("can detect no charset string", () => {
            for (const sample of allNoCharsetSamples()) {
                assert(sample.charset === ShioriEncodeLayer.getStringCharset(sample.content));
            }
        });
    });

    describe("getArrayBufferCharset()", () => {
        it("can detect string", () => {
            for (const sample of allHasCharsetSamples(true)) {
                assert(sample.charset === ShioriEncodeLayer.getArrayBufferCharset(sample.content));
            }
        });

        it("can detect no charset string", () => {
            for (const sample of allNoCharsetSamples(true)) {
                assert(sample.charset === ShioriEncodeLayer.getArrayBufferCharset(sample.content));
            }
        });
    });
});