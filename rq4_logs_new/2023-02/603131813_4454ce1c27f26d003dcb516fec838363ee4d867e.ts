import { type Response, type Request, NextFunction } from "express";
import CustomError from "../CustomError";
import auth from "./auth";
import { generalError } from "./errorMiddlewares";

describe("Given an auth function", () => {
  describe("When it receives an object req and its method header without 'Authorization' value'", () => {
    test("Then it shoul return the status 401", () => {
      const errorStatusCode = 401;
      const req = { header: jest.fn() } as Partial<Request>;
      const res = {} as Response;
      const next = jest.fn();
      const error = new CustomError("", errorStatusCode, "");
      const mockResponse = {
        status: jest.fn().mockImplementation(() => ({ json: jest.fn() })),
      } as Partial<Response>;

      next.mockImplementation(() => {
        generalError(error, req as Request, mockResponse as Response, next);
      });
      auth(req as Request, res, next);

      expect(mockResponse.status).toHaveBeenCalledWith(errorStatusCode);
    });
  });

  describe("When it receives an object req and its method header with 'Authorization' value that not includes 'Bearer'", () => {
    test("Then it should return the status 401", () => {
      const errorStatusCode = 401;
      const req = { header: jest.fn() } as Partial<Request>;
      const res = {} as Response;
      const next = jest.fn();
      const error = new CustomError("", errorStatusCode, "");
      const mockResponse = {
        status: jest.fn().mockImplementation(() => ({ json: jest.fn() })),
      } as Partial<Response>;

      req.header = jest.fn().mockImplementation(() => "myAuthToken");
      next.mockImplementation(() => {
        generalError(error, req as Request, mockResponse as Response, next);
      });
      auth(req as Request, res, next);

      expect(req.header).toHaveBeenCalledWith("Authorization");
      expect(mockResponse.status).toHaveBeenCalledWith(errorStatusCode);
    });
  });
});