import { CreateBudget } from "../../domain/useCases/create-budget";
import { InvalidParamError } from "../errors/invalid-param-error";
import { MissingParamError } from "../errors/missing-param-error";
import { badRequest, serverError } from "../helpers/http-helper";
import { Controller } from "../protocols/controller";
import { HttpRequest, HttpResponse } from "../protocols/http";

export class CreateBudgetController implements Controller {
  constructor(private createBudget: CreateBudget) {}

  async handle(httpRequest: HttpRequest): Promise<HttpResponse> {
    try {
      const requiredFields = ["userId", "productsId"];
      for (const field of requiredFields) {
        if (!httpRequest.body[field]) {
          return badRequest(new MissingParamError(field))
        }
      }

      const { userId, productsId } = httpRequest.body;
      if (typeof userId !== "number") {
        return badRequest(new InvalidParamError("userId"))
      }

      if (!Array.isArray(productsId)) {
        return badRequest(new InvalidParamError("productsId"))
      } else {
        for (const id of productsId) {
          if (typeof id !== "number") {
            return badRequest(new InvalidParamError("productsId"))
          }
        }
      }

      const budget = await this.createBudget.create(userId, productsId);
      return {
        statusCode: 201,
        body: budget
      }
    } catch (error) {
      return serverError();
    }
  }
}