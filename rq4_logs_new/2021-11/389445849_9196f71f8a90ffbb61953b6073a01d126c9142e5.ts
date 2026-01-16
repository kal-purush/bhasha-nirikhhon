import { BcryptAdapter } from "@/infra/hash/bcrypt-adapter";

const makeSut = () => {
  const sut = new BcryptAdapter();
  return {
    sut,
  };
};

describe("==> hash adapter implementation", () => {
  it("Should return a hashed plaintext", async () => {
    const { sut } = makeSut();

    const plaintext = "password";
    const hashedPlaintext = await sut.hash(plaintext);

    expect(hashedPlaintext).toBeTruthy();
  });

  it("Should return a true if plaintext is valid", async () => {
    const { sut } = makeSut();

    const plaintext = "password";
    const hashedPlaintext = await sut.hash(plaintext);
    const comparePlaintext = await sut.compare(plaintext, hashedPlaintext);

    expect(comparePlaintext).toBe(true);
  });
});