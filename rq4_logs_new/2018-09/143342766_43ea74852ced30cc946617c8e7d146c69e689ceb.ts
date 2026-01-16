import { SRLatchOutput } from "../hackjs";
import { SRLatch } from "./latch";

describe("latches", () => {

  describe("SRLatch", () => {
    it("can hold state", () => {
      expect(SRLatch(0, 0, { q: 0, nq: 1 })).toEqual({ q: 0, nq: 1 });
      expect(SRLatch(0, 0, { q: 1, nq: 0 })).toEqual({ q: 1, nq: 0 });
    });
    it("can reset (ie set Q to 0)", () => {
      expect(SRLatch(0, 1, { q: 0, nq: 1 })).toEqual({ q: 0, nq: 1 });
      expect(SRLatch(0, 1, { q: 1, nq: 0 })).toEqual({ q: 0, nq: 1 });
    });
    it("can set (ie set Q to 1)", () => {
      expect(SRLatch(1, 0, { q: 0, nq: 1 })).toEqual({ q: 1, nq: 0 });
      expect(SRLatch(1, 0, { q: 1, nq: 0 })).toEqual({ q: 1, nq: 0 });
    });
    it("can validate a simple set/latch/reset/latch flow", () => {
      let output: SRLatchOutput = { q: 0, nq: 1 };

      // Q is set.
      output = SRLatch(1, 0, output);
      expect(output).toEqual({ q: 1, nq: 0 });

      // Keep 1.
      output = SRLatch(0, 0, output);
      expect(output).toEqual({ q: 1, nq: 0 });

      // Q is reset.
      output = SRLatch(0, 1, output);
      expect(output).toEqual({ q: 0, nq: 1 });

      // Latch on.
      output = SRLatch(0, 0, output);
      expect(output).toEqual({ q: 0, nq: 1 });

      // Set to invalid output (both set and reset bits).
      output = SRLatch(1, 1, output);
      expect(output).toEqual({ q: 0, nq: 1 });
    });
  });

});