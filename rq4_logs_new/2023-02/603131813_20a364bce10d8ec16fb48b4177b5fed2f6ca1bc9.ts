import type { Request, NextFunction, Response } from "express";
import CustomError from "../CustomError";
import { generalError } from "./errorMiddlewares";

describe("Given a generalError middleware", () => {
  describe("When it receives an error with status code 404", () => {
    test("Then it should return a response with that status code", async () => {
      const errorMessage = "Page not found";
      const mockRequest = {} as Request;
      const errorStatusCode = 404;
      const mockResponse = {
        status: jest.fn().mockImplementation(() => ({
          json: jest.fn().mockResolvedValue(errorStatusCode),
        })),
      } as Partial<Response>;
      const nextFunction: NextFunction = jest.fn();
      const error = new CustomError(
        "Resource not found",
        errorStatusCode,
        errorMessage
      );

      generalError(error, mockRequest, mockResponse as Response, nextFunction);

      expect(mockResponse.status).toHaveBeenCalledWith(errorStatusCode);
    });
  });
});