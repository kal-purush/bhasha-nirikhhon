import { syllabler } from "./syllabler";

const examples = [
  { in: "mountainous", out: ["moun", "tain", "ous"] },
  { in: "villainous", out: ["vil", "lain", "ous"] },
  { in: "betrayal", out: ["be", "tray", "al"] },
  { in: "defrayal", out: ["de", "fray", "al"] },
  { in: "portrayal", out: ["por", "tray", "al"] },
  { in: "hearken", out: ["hear", "ken"] },
  { in: "extremely", out: ["ex", "treme", "ly"] },
  { in: "supremely", out: ["su", "preme", "ly"] },
  { in: "toothaches", out: ["tooth", "aches"] },
  { in: "bachelor", out: ["bach", "e", "lor"] },
  { in: "echelon", out: ["ech", "e", "lon"] },
  { in: "riffraff", out: ["riff", "raff"] },
  { in: "analogous", out: ["anal", "o", "gous"] },
  { in: "homologous", out: ["ho", "mol", "o", "gous"] },
  { in: "genuine", out: ["gen", "u", "ine"] },
  { in: "anyplace", out: ["any", "place"] },
  { in: "coalesce", out: ["co", "a", "lesce"] },
  { in: "forewarn", out: ["fore", "warn"] },
  { in: "foreword", out: ["fore", "word"] },
  { in: "despair", out: ["de", "spair"] },
  { in: "antarctic", out: ["ant", "arc", "tic"] },
  { in: "cornstarch", out: ["corn", "starch"] },
  { in: "mastodon", out: ["mast", "odon"] },
  { in: "squirmed", out: ["squirmed"] }
];

describe("syllabler", () => {
  describe("split method", () => {
    it("should split words into syllables", () => {
      expect(syllabler.split("water")).toEqual(["water"]);
    });
  });
});