
export type Alphabet = Letter[];

export type Letter = {
  fast: string;
  slow: string;
  letter: string;
  phonetic: string;
  example: string;
};

export const alphabet: Alphabet = [
  { slow: "/mmm.m4a", fast: "/m.m4a", letter: "m", phonetic: "mmm", example: "ram" },
  { slow: "/sss.m4a", fast: "/s.m4a", letter: "s", phonetic: "sss", example: "bus" },
  { slow: "/aaa.m4a", fast: "/a.m4a", letter: "a", phonetic: "aaa", example: "and" },
  { slow: "/ēēē.m4a", fast: "/ē.m4a", letter: "ē", phonetic: "ēēē", example: "eat" },
  { slow: "/t.m4a", fast: "/t.m4a", letter: "t", phonetic: "t", example: "cat" },
  { slow: "/rrr.m4a", fast: "/r.m4a", letter: "r", phonetic: "rrr", example: "bar" },
  { slow: "/d.m4a", fast: "/d.m4a", letter: "d", phonetic: "d", example: "mad" },
  { slow: "/iii.m4a", fast: "/i.m4a", letter: "i", phonetic: "iii", example: "if" },
  { slow: "/ththth.m4a", fast: "/th.m4a", letter: "th", phonetic: "ththth", example: "this (not bathe)" },
  { slow: "/k.m4a", fast: "/k.m4a", letter: "c", phonetic: "c", example: "tack" },
  { slow: "/ooo.m4a", fast: "/o.m4a", letter: "o", phonetic: "ooo", example: "ox" },
  { slow: "/nnn.m4a", fast: "/n.m4a", letter: "n", phonetic: "nnn", example: "pan" },
  { slow: "/fff.m4a", fast: "/f.m4a", letter: "f", phonetic: "fff", example: "stuff" },
  { slow: "/uuu.m4a", fast: "/u.m4a", letter: "u", phonetic: "uuu", example: "under" },
  { slow: "/lll.m4a", fast: "/l.m4a", letter: "l", phonetic: "lll", example: "pal" },
  { slow: "/www.m4a", fast: "/w.m4a", letter: "w", phonetic: "www", example: "wow" },
  { slow: "/g.m4a", fast: "/g.m4a", letter: "g", phonetic: "g", example: "tag" },
  { slow: "/I.m4a", fast: "/I.m4a", letter: "I", phonetic: "(the word I)", example: "(the word I)" },
  { slow: "/shshsh.m4a", fast: "/sh.m4a", letter: "sh", phonetic: "shshsh", example: "wish" },
  { slow: "/āāā.m4a", fast: "/ā.m4a", letter: "ā", phonetic: "āāā", example: "ate" },
  { slow: "/h.m4a", fast: "/h.m4a", letter: "h", phonetic: "h", example: "hat" },
  { slow: "/k.m4a", fast: "/k.m4a", letter: "k", phonetic: "k", example: "tack" },
  { slow: "/ōōō.m4a", fast: "/ō.m4a", letter: "ō", phonetic: "ōōō", example: "over" },
  { slow: "/vvv.m4a", fast: "/v.m4a", letter: "v", phonetic: "vvv", example: "love" },
  { slow: "/p.m4a", fast: "/p.m4a", letter: "p", phonetic: "p", example: "sap" },
  { slow: "/arrrr.m4a", fast: "/ar.m4a", letter: "ar", phonetic: "arrrr", example: "car" },
  { slow: "/ch.m4a", fast: "/ch.m4a", letter: "ch", phonetic: "ch", example: "touch" },
  { slow: "/eee.m4a", fast: "/e.m4a", letter: "e", phonetic: "eee", example: "end" },
  { slow: "/b.m4a", fast: "/b.m4a", letter: "b", phonetic: "b", example: "grab" },
  { slow: "/iiing.m4a", fast: "/ing.m4a", letter: "ing", phonetic: "iiing", example: "sing" },
  { slow: "/īīī.m4a", fast: "/ī.m4a", letter: "ī", phonetic: "īīī", example: "ice" },
  { slow: "/yyyē.m4a", fast: "/y.m4a", letter: "y", phonetic: "yyyē", example: "yard" },
  { slow: "/urrr.m4a", fast: "/er.m4a", letter: "er", phonetic: "urrr", example: "brother" },
  { slow: "/oooooo.m4a", fast: "/oo.m4a", letter: "oo", phonetic: "ooooooo", example: "moon" },
  { slow: "/j.m4a", fast: "/j.m4a", letter: "j", phonetic: "j", example: "judge" },
  { slow: "/whwhwh.m4a", fast: "/wh.m4a", letter: "wh", phonetic: "whwhwh", example: "why" },
  { slow: "/īīī.m4a", fast: "/y̅.m4a", letter: "y̅", phonetic: "īīī", example: "my" },
  { slow: "/ūūū.m4a", fast: "/ū.m4a", letter: "ū", phonetic: "ūūū", example: "use" },
  { slow: "/kwww.m4a", fast: "/qu.m4a", letter: "qu", phonetic: "kwww", example: "quick" },
  { slow: "/xsss.m4a", fast: "/x.m4a", letter: "x", phonetic: "xsss", example: "ox" },
  { slow: "/zzz.m4a", fast: "/z.m4a", letter: "z", phonetic: "zzz", example: "buzz" },
  { slow: "/ēēē.m4a", fast: "/ae.m4a", letter: "ae", phonetic: "ēēē", example: "leave" },
  { slow: "/āāā.m4a", fast: "/ai.m4a", letter: "ai", phonetic: "āāā", example: "rain" },
  { slow: "/owww.m4a", fast: "/ou.m4a", letter: "ou", phonetic: "owww", example: "loud" },
].map(a => {
  if (encodeURI(a.slow) != a.slow) {

    a.slow = a.slow.replace("/", "/_").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  }
  return a;
});