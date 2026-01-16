import pg from "pg";
import { Trade } from "../../../src/server/types";
import Controller from "./../../../src/server/controller/plugin";

describe("Controllers Test", function () {
  jest.mock("pg");

  const controller = new Controller();
  const trade: Trade = {
    id: "abc",
    ticker: "ABC",
    company_name: "Test",
    reference_number: "",
    unit_price: 0,
    quantity: 0,
    total_cost: 0,
    trade_type: "long",
    note: "",
    created_at: "10/10/2020",
    trade_status: true,
  };

  describe("Write Action", function () {
    beforeAll(function () {
      pg.Client.prototype.query = jest.fn().mockResolvedValue({
        rows: [
          {
            id: "abc",
          },
        ],
      });
    });

    afterAll(function () {
      jest.resetAllMocks();
    });

    it("should write a trade to db and return an id", async function () {
      const result: {
        [rows: string]: [{ [id: string]: "" }];
      } = await controller.write(trade);
      expect(result.rows[0].id).toEqual("abc");
      expect(
        pg.Client.prototype.query
      ).toHaveBeenCalledWith(
        `INSERT INTO trades (id, ticker, company_name, reference_number, unit_price, quantity, total_cost, trade_type, note, created_at, trade_status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`,
        ["abc", "ABC", "Test", "", 0, 0, 0, "long", "", "10/10/2020", true]
      );
    });
  });

  describe("Read Action", function () {
    beforeAll(function () {
      pg.Client.prototype.query = jest.fn().mockResolvedValue(trade);
    });

    afterAll(function () {
      jest.resetAllMocks();
    });

    it("should return a document given an id", async function () {
      const result = await controller.read(trade.id);
      expect(result).toEqual(trade);
      expect(
        pg.Client.prototype.query
      ).toHaveBeenCalledWith(`SELECT * FROM trades WHERE id = $1`, ["abc"]);
    });
  });

  describe("Read Action by Ticker", function () {
    beforeAll(function () {
      pg.Client.prototype.query = jest.fn().mockResolvedValue(trade);
    });

    afterAll(function () {
      jest.resetAllMocks();
    });

    it("should return a document given a ticker", async function () {
      const result = await controller.readByTicker(trade.ticker);
      expect(result).toEqual(trade);
      expect(
        pg.Client.prototype.query
      ).toHaveBeenCalledWith(`SELECT * FROM trades WHERE ticker = $1`, ["ABC"]);
    });
  });

  describe("Read Action by Company Name", function () {
    beforeAll(function () {
      pg.Client.prototype.query = jest.fn().mockResolvedValue(trade);
    });

    afterAll(function () {
      jest.resetAllMocks();
    });

    it("should return a document given a company name", async function () {
      const result = await controller.readByCompany(trade.company_name);
      expect(result).toEqual(trade);
      expect(
        pg.Client.prototype.query
      ).toHaveBeenCalledWith(`SELECT * FROM trades WHERE company_name = $1`, ["Test"]);
    });
  });

  describe("Read Action by Date", function () {
    beforeAll(function () {
      pg.Client.prototype.query = jest.fn().mockResolvedValue(trade);
    });

    afterAll(function () {
      jest.resetAllMocks();
    });

    it("should return a document given a date", async function () {
      const result = await controller.readByDate(trade.created_at);
      expect(result).toEqual(trade);
      expect(
        pg.Client.prototype.query
      ).toHaveBeenCalledWith(`SELECT * FROM trades WHERE created_at = $1`, ["10/10/2020"]);
    });
  });
});