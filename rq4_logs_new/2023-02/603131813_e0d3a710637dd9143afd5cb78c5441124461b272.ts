import { type Request, type Response } from "express";
import Robot from "../../database/models/Robot";
import { getRobots } from "./robotsControllers";

describe("Given the getRobots function", () => {
  describe("When it receives a res object", () => {
    test("Then it should call its 'status' method with 200", async () => {
      const req = {} as Request;
      const res = { status: jest.fn() } as Partial<Response>;
      const next = jest.fn();
      const statusCode = 200;

      Robot.find = jest.fn().mockImplementationOnce(() => ({
        exec: jest.fn().mockResolvedValue({
          stats: {
            speed: 8,
            endurance: 6,
            creationDate: "1989-12-26T23:00:00.000Z",
          },
          _id: "63efcf24773d3719e628a284",
          name: "AstroBot",
          image: "image.png",
        }),
      }));

      await getRobots(req, res as Response, next);

      expect(res.status).toHaveBeenCalledWith(statusCode);
    });
  });
});