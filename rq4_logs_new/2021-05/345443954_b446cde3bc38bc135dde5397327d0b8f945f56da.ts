import { expect } from "chai";
import app from "../src/server";
import { agent as request } from "supertest";

const email = "admin@admin.com";
const bookId = "6085db376f3e934e94983170";

describe("Test for favorites", async () => {
    it("should POST /api/favorite/add", async function () {
        const res = await request(app).post("/api/favorite/add").send({
            email: email,
            bookId: bookId,
        });
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });

    it("should POST /api/favorite/remove", async function () {
        const res = await request(app).post("/api/favorite/remove").send({
            email: email,
            bookId: bookId,
        });
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });
});