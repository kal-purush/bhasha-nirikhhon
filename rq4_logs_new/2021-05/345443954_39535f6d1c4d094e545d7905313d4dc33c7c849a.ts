import { expect } from "chai";
import app from "../src/server";
import { agent as request } from "supertest";

const id = "607c36ea053a6512e81acf67";
const unit = "unit";
let postedId = "";

describe("Books test", async () => {
    it("should GET /api/books", async function () {
        const res = await request(app).get("/api/books");
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
        expect(res.body).to.be.an("array");
    });

    it("should GET /api/books/:id", async function () {
        const res = await request(app).get(`/api/books/${id}`);
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });

    it("should POST /api/books", async function () {
        const res = await request(app).post("/api/books").send({
            title: "Unit test",
            author: "Unit test",
            genre: "Unit test",
            description: "Unit test and delete",
            cover: "",
        });
        postedId = res.body._id;
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });

    it("should GET /api/search/:query", async function () {
        const res = await request(app).get(`/api/search/${unit}`);
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });

    it("should DELETE /api/books/:id", async function () {
        const res = await request(app).delete(`/api/books/${postedId}`);
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });
});