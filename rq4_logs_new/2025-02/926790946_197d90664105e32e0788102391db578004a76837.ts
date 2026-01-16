import bcrypt from "bcrypt";
import Hasher from "@/lib/Hasher";

jest.mock("bcrypt");

describe("src/lib/Hasher.ts", () => {
  let hasher: Hasher;

  beforeEach(() => {
    hasher = new Hasher();
  });

  describe("Successful Cases", () => {
    test("Should encrypt data", async () => {
      (bcrypt.hash as jest.Mock).mockResolvedValue("hashed-data");

      const hashedData = await hasher.encrypt("data");

      expect(bcrypt.hash).toHaveBeenCalledWith("data", 12);
      expect(hashedData).toBe("hashed-data");
    });

    test("Shold compare data and hashed data and return true", async () => {
      (bcrypt.compare as jest.Mock).mockResolvedValue(true);

      const data = await hasher.decrypt("data", "hashed-data");

      expect(bcrypt.compare).toHaveBeenCalledWith("data", "hashed-data");
      expect(data).toBe(true);
    });
  });

  describe("Failure Cases", () => {
    test("Shold compare data and hashed data and return false", async () => {
      (bcrypt.compare as jest.Mock).mockResolvedValue(false);

      const data = await hasher.decrypt("wrong-data", "wrong-hashed-data");

      expect(bcrypt.compare).toHaveBeenCalledWith("wrong-data", "wrong-hashed-data");
      expect(data).toBe(false);
    });
  });
});