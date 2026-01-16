import { expect } from "chai";
import app from "../src/server";
import { agent as request } from "supertest";

let token = "";
const email = "unittest@test.com";

describe("Admin test", async () => {
    it("should POST /api/login", async function () {
        const res = await request(app).post("/api/login").send({
            email: "admin@admin.com",
            password: "admin",
        });
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });

    it("should POST /api/register", async function () {
        const res = await request(app).post("/api/register").send({
            email: "unittest@test.com",
            password: "unittest",
        });
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });

    it("should POST /api/current-user/:email", async function () {
        const res = await request(app).post(`/api/current-user/${email}`);
        expect(res.status).to.equal(200);
        expect(res.body).not.to.be.empty;
    });
});