import { formatAccount, formatTlf } from "../formatters";

describe("formatters", () => {
  describe("formatAccount", () => {
    it("handles empty", () => {
      const account = formatAccount();

      expect(account).toBe();
    });

    it("handles empty string", () => {
      const account = formatAccount("");

      expect(account).toBe("");
    });

    it("handles invalid string", () => {
      const account = formatAccount("WhoopDeDoo");

      expect(account).toBe("WhoopDeDoo");
    });

    it("formats account", () => {
      const account = formatAccount("12345678901");

      expect(account).toBe("1234.56.78901");
    });

    it("does not break formmating on formatted account", () => {
      const account = formatAccount("1234.56.78901");

      expect(account).toBe("1234.56.78901");
    });
  });

  describe("formatTlf", () => {
    it("handles empty ", () => {
      const tlf = formatTlf();

      expect(tlf).toBe();
    });

    it("handles empty string", () => {
      const tlf = formatTlf("");

      expect(tlf).toBe("");
    });

    it("handles invalid string", () => {
      const tlf = formatTlf("WhoopDeDoo");

      expect(tlf).toBe("WhoopDeDoo");
    });

    it("formats mobile numbers", () => {
      const tlf4 = formatTlf("42345678");
      const tlf8 = formatTlf("82345678");
      const tlf9 = formatTlf("92345678");

      expect(tlf4).toBe("+47 423 45 678");
      expect(tlf8).toBe("+47 823 45 678");
      expect(tlf9).toBe("+47 923 45 678");
    });

    it("fixed mobile numbers formatted as non-mobile", () => {
      const tlf4 = formatTlf("42 34 56 78");
      const tlf8 = formatTlf("82 34 56 78");
      const tlf9 = formatTlf("+47 92 34 56 78");

      expect(tlf4).toBe("+47 423 45 678");
      expect(tlf8).toBe("+47 823 45 678");
      expect(tlf9).toBe("+47 923 45 678");
    });

    it("does not break formatting on formatted mobile numbers", () => {
      const tlf4 = formatTlf("+47 423 45 678");
      const tlf8 = formatTlf("+47 823 45 678");
      const tlf9 = formatTlf("+47 923 45 678");

      expect(tlf4).toBe("+47 423 45 678");
      expect(tlf8).toBe("+47 823 45 678");
      expect(tlf9).toBe("+47 923 45 678");
    });

    it("formats non-mobile numbers", () => {
      const tlf0 = formatTlf("02345678");
      const tlf1 = formatTlf("12345678");
      const tlf2 = formatTlf("22345678");
      const tlf3 = formatTlf("32345678");
      const tlf5 = formatTlf("52345678");
      const tlf6 = formatTlf("62345678");
      const tlf7 = formatTlf("72345678");

      expect(tlf0).toBe("+47 02 34 56 78");
      expect(tlf1).toBe("+47 12 34 56 78");
      expect(tlf2).toBe("+47 22 34 56 78");
      expect(tlf3).toBe("+47 32 34 56 78");
      expect(tlf5).toBe("+47 52 34 56 78");
      expect(tlf6).toBe("+47 62 34 56 78");
      expect(tlf7).toBe("+47 72 34 56 78");
    });

    it("fixes non-mobile numbers formatted as mobile", () => {
      const tlf0 = formatTlf("023 45 678");
      const tlf1 = formatTlf("123 45 678");
      const tlf2 = formatTlf("223 45 678");
      const tlf3 = formatTlf("+47 323 45 678");
      const tlf5 = formatTlf("+47 523 45 678");
      const tlf6 = formatTlf("+47 623 45 678");
      const tlf7 = formatTlf("+47 723 45 678");

      expect(tlf0).toBe("+47 02 34 56 78");
      expect(tlf1).toBe("+47 12 34 56 78");
      expect(tlf2).toBe("+47 22 34 56 78");
      expect(tlf3).toBe("+47 32 34 56 78");
      expect(tlf5).toBe("+47 52 34 56 78");
      expect(tlf6).toBe("+47 62 34 56 78");
      expect(tlf7).toBe("+47 72 34 56 78");
    });

    it("does not break formatting on formatted non-mobile numbers", () => {
      const tlf0 = formatTlf("+47 02 34 56 78");
      const tlf1 = formatTlf("+47 12 34 56 78");
      const tlf2 = formatTlf("+47 22 34 56 78");
      const tlf3 = formatTlf("+47 32 34 56 78");
      const tlf5 = formatTlf("+47 52 34 56 78");
      const tlf6 = formatTlf("+47 62 34 56 78");
      const tlf7 = formatTlf("+47 72 34 56 78");

      expect(tlf0).toBe("+47 02 34 56 78");
      expect(tlf1).toBe("+47 12 34 56 78");
      expect(tlf2).toBe("+47 22 34 56 78");
      expect(tlf3).toBe("+47 32 34 56 78");
      expect(tlf5).toBe("+47 52 34 56 78");
      expect(tlf6).toBe("+47 62 34 56 78");
      expect(tlf7).toBe("+47 72 34 56 78");
    });
  });
});