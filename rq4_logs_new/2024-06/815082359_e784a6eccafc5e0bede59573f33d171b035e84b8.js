import chai from "chai";
const chaiHttp = require("chai-http");
const app = require("../app"); // Modifiez ceci pour exporter l'application dans app.js
const Task = require("../models/task"); // Modifiez ceci pour utiliser le modèle de tâche

const expect = chai.expect;
chai.use(chaiHttp);

describe("Task API", () => {
  beforeEach(async () => {
    await Task.deleteMany({});
  });

  it("should create a new task", (done) => {
    chai
      .request(app)
      .post("/tasks")
      .send({ name: "Test task" })
      .end((err, res) => {
        expect(res).to.have.status(200);
        expect(res.body).to.have.property("name", "test task");
        done();
      });
  });

  // Ajoutez d'autres tests pour Read, Update et Delete
});