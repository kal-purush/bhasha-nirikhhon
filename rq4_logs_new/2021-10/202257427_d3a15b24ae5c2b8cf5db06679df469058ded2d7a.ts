import { getImageSrcset } from "../getImageSrcset";

const BASE = "https://res.cloudinary.com/pirosikick/image/upload";
const V = "v000000000";
const JPG = "a/b/c.jpg";
const WEBP = "a/b/c.webp";

test.each([
  {
    url: "https://example.com/images/upload/hoge.jpg",
    ext: undefined,
    expected: undefined,
  },
  // without transformation options
  {
    url: `${BASE}/${V}/${JPG}`,
    ext: undefined,
    expected: `${BASE}/c_limit,w_160/${V}/${JPG} 160w, ${BASE}/c_limit,w_320/${V}/${JPG} 320w`,
  },
  // with transformation options
  {
    url: `${BASE}/c_limit,w_160/${V}/${JPG}`,
    ext: undefined,
    expected: `${BASE}/c_limit,w_160/${V}/${JPG} 160w, ${BASE}/c_limit,w_320/${V}/${JPG} 320w`,
  },
  // webp
  {
    url: `${BASE}/${V}/${JPG}`,
    ext: ".webp",
    expected: `${BASE}/c_limit,w_160/${V}/${WEBP} 160w, ${BASE}/c_limit,w_320/${V}/${WEBP} 320w`,
  },
])("getImageSrcset($url, $ext) === $expected", ({ url, ext, expected }) => {
  expect(getImageSrcset(url, ext, [160, 320])).toBe(expected);
});