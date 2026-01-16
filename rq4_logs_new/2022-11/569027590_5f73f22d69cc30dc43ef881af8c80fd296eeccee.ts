import youtubeUrlToID from "./youtubeUrlToID";

describe("youtube url to id", () => {
  it("handles short url", () => {
    expect(youtubeUrlToID("https://youtu.be/ZFqlHhCNBOI")).toBe("ZFqlHhCNBOI");
    expect(youtubeUrlToID("https://youtu.be/mhYwzSHdo_I")).toBe("mhYwzSHdo_I");
  });
  it("handles watch url", () => {
    expect(youtubeUrlToID("https://www.youtube.com/watch?v=mhYwzSHdo_I")).toBe(
      "mhYwzSHdo_I"
    );
    expect(youtubeUrlToID("https://www.youtube.com/watch?v=xcJtL7QggTI")).toBe(
      "xcJtL7QggTI"
    );
  });
  it("handles watch url with extra parameter", () => {
    expect(
      youtubeUrlToID("https://www.youtube.com/watch?v=mhYwzSHdo_I&ref=test")
    ).toBe("mhYwzSHdo_I");
  });
});