import { VirtualAlexa } from "virtual-alexa";
import { handler } from "../src";

describe("Distance Handler", () => {
  let alexa: VirtualAlexa;
  beforeEach(() => {
    alexa = VirtualAlexa.Builder()
      .handler(handler)
      .interactionModelFile("models/de-DE.json")
      .create();
    alexa.filter((requestJSON) => {
      requestJSON.request.locale = "de-DE";
    });
  });

  test("missing both slots", async () => {
    const result = await alexa.request()
      .intent("DistanceIntent")
      .send();

    expect(result.response.outputSpeech.ssml).toContain("Bitte nenne zwei Städte.");
    expect(result.response.shouldEndSession).toBe(false);
  });

  test("missing 'from' slot", async () => {
    const result = await alexa.request()
      .intent("DistanceIntent")
      .slot("to", "berlin")
      .send();

    expect(result.response.outputSpeech.ssml).toContain("Bitte nenne zwei Städte.");
    expect(result.response.shouldEndSession).toBe(false);
  });

  test("missing 'to' slot", async () => {
    const result = await alexa.request()
      .intent("DistanceIntent")
      .slot("from", "berlin")
      .send();

    expect(result.response.outputSpeech.ssml).toContain("Bitte nenne zwei Städte.");
    expect(result.response.shouldEndSession).toBe(false);
  });

  test("equal 'from' and 'to' values", async () => {
    const result = await alexa.request()
      .intent("DistanceIntent")
      .slot("from", "berlin")
      .slot("to", "berlin")
      .send();

    expect(result.response.outputSpeech.ssml).toContain("Bitte nenne zwei unterschiedliche Städte.");
    expect(result.response.shouldEndSession).toBe(false);
  });

  test("invalid cities", async () => {
    const result = await alexa.request()
      .intent("DistanceIntent")
      .slot("from", "invalid-city-one")
      .slot("to", "invalid-city-two")
      .send();

    expect(result.response.outputSpeech.ssml)
      .toContain("Ich konnte die Entfernung zwischen invalid-city-one und invalid-city-two nicht berechnen.");
    expect(result.response.shouldEndSession).toBe(false);
  });

  test("valid cities", async () => {
    const result = await alexa.request()
      .intent("DistanceIntent")
      .slot("from", "hamburg")
      .slot("to", "berlin")
      .send();

    expect(result.response.outputSpeech.ssml).toContain("Die Entfernung zwischen hamburg und berlin beträgt");
    expect(result.response.shouldEndSession).toBe(true);
  });
});