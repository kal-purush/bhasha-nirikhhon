import { ValidatorAdapter } from "@/infra/email-validator/validator-adapter";

const makeSut = () => {
  const sut = new ValidatorAdapter();
  return {
    sut,
  };
};

describe("==> email validator adapter implementation", () => {
  it("Should return true if email is valid", async () => {
    const { sut } = makeSut();

    const email = "email123@email.com";
    const isEmail = await sut.isValid(email);

    expect(isEmail).toBe(true);
  });

  it("Should return false if email is valid", async () => {
    const { sut } = makeSut();

    const email = "invalid.email.com";
    const isEmail = await sut.isValid(email);

    expect(isEmail).toBe(false);
  });
});