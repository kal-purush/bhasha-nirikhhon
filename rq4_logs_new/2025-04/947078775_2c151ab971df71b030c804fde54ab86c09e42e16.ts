import { ArgList } from "./types";

const getObject = (
  url: string,
  artist: string,
  titleIndex: number,
  tjIndex: number,
  kyIndex: number
): ArgList => {
  return { url, artist, titleIndex, tjIndex, kyIndex };
};

// 테이블 구조 상 불가능했던 문서
// aiko, AKB48, 노기자카46, 니시노 카나, 모닝구 무스메, Sound Horizon, 오오츠카 아이, 케야키자카46
// 너무 적은 데이터
// &TEAM, NiziU

export const argList = [
  // url, artist, titleIndex, tjIndex, kyIndex
  getObject("AAA(혼성그룹)", "AAA", 0, 3, 2),
  getObject("Aimer", "에이머(Aimer)", 2, 0, 1),
  getObject("amazarashi", "아마자라시(amazarashi)", 0, 1, 2),
  getObject("BUMP OF CHICKEN", "BUMP OF CHICKEN", 0, 1, 2),
  getObject("DREAMS COME TRUE(밴드)", "DREAMS COME TRUE", 0, 1, 2),
  getObject("ELLEGARDEN", "엘르가든(ELLEGARDEN)", 0, 1, 2),
  getObject("King Gnu", "킹누(King Gnu)", 0, 1, 2),
  getObject("LiSA", "리사(LiSA)", 2, 0, 1),
  getObject("Mrs. GREEN APPLE", "미세스그린애플(Mrs. GREEN APPLE)", 0, 1, 2),
  getObject("Official髭男dism", "오피셜히게단디즘(Official髭男dism)", 2, 0, 1),
  getObject("Perfume", "퍼퓸(Perfume)", 0, 1, 2),
  getObject("RADWIMPS", "래드윔프스(RADWIMPS)", 2, 0, 1),
  getObject("SEKAI NO OWARI", "세카이노오와리(SEKAI NO OWARI)", 0, 1, 2),
  getObject("SPYAIR", "스파이에어(SPYAIR)", 2, 0, 1),
  getObject("Vaundy", "바운디(Vaundy)", 2, 0, 1),
  getObject("w-inds.", "w-inds.", 0, 1, 2),
  getObject("YOASOBI", "요아소비(YOASOBI)", 0, 1, 2),
  getObject(
    "계속 한밤중이면 좋을 텐데.",
    "계속 한밤중이면 좋을 텐데.(즛토마요나카데이이노니)",
    2,
    0,
    1
  ),
  getObject("베리즈코보", "베리즈코보", 0, 1, 2),
  getObject("아라시(아이돌)", "아라시", 0, 1, 2),
  getObject("아이묭", "아이묭(あいみょん)", 0, 1, 2),
  getObject("요네즈 켄시", "요네즈 켄시", 2, 0, 1),
  getObject("요루시카", "요루시카(ヨルシカ)", 0, 1, 2),
  getObject("유이카", "유이카", 0, 1, 2),
  getObject("호시노 겐", "호시노 겐", 0, 1, 2),
  getObject("Creepy Nuts", "크리피 넛츠(Creepy Nuts)", 2, 0, 1),
];