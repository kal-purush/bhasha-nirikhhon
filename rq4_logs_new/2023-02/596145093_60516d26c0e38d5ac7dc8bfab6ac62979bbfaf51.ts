import { MissingParamError } from "../errors/missing-param-error";
import { CreateBudgetController } from "./create-budget";
import { CreateBudget } from "../../domain/useCases/create-budget";
import { InvalidParamError } from "../errors/invalid-param-error";

interface ControllerTypes {
  sut: CreateBudgetController
  createBudgeStub: CreateBudget
}

const budge = 1910;

const makeCreateBudge = (): CreateBudget => {
  class CreateBudgeStub implements CreateBudget {
    create(userId: number, productsId: number[]): Promise<number> {
      return new Promise(resolve => resolve(budge));
    }
  }

  return new CreateBudgeStub();
}

const makeSut = (): ControllerTypes => {
  const createBudgeStub = makeCreateBudge();
  const sut = new CreateBudgetController(createBudgeStub);

  return {
    sut,
    createBudgeStub
  }
}

describe("CreateBudget Controller", () => {
  it("Should return 400 if no userId is provided", async () => {
    const { sut } = makeSut();
    const httpResquest = {
      body: {
        productsId: [1, 2]
      }
    }

    const httpResponse = await sut.handle(httpResquest);

    expect(httpResponse.statusCode).toBe(400);
    expect(httpResponse.body).toEqual(new MissingParamError('userId'));
  })

  it("Should return 400 if no productsId is provided", async () => {
    const { sut } = makeSut();
    const httpResquest = {
      body: {
        userId: 5
      }
    }

    const httpResponse = await sut.handle(httpResquest);

    expect(httpResponse.statusCode).toBe(400);
    expect(httpResponse.body).toEqual(new MissingParamError('productsId'));
  })

  it("Should return 400 if userId is invalid", async () => {
    const { sut } = makeSut();
    const httpResquest = {
      body: {
        userId: "5",
        productsId: [1, 2]
      }
    }

    const httpResponse = await sut.handle(httpResquest);

    expect(httpResponse.statusCode).toBe(400);
    expect(httpResponse.body).toEqual(new InvalidParamError('userId'));
  })

  it("Should return 400 if productsId is invalid", async () => {
    const { sut } = makeSut();
    const httpResquest = {
      body: {
        userId: 5,
        productsId: [1, "2"]
      }
    }

    const httpResponse = await sut.handle(httpResquest);

    expect(httpResponse.statusCode).toBe(400);
    expect(httpResponse.body).toEqual(new InvalidParamError('productsId'));
  })
})