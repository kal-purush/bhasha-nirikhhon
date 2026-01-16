const express = require("express");

const projectsDbHelper = require("../data/helpers/projectModel");
const actionsDbHelper = require("../data/helpers/actionModel");

const actionRouter = express.Router();

actionRouter.get("/", (req, res) => {
  actionsDbHelper
    .get()
    .then(actions => res.status(201).json(actions))
    .catch(err =>
      res.status(500).json({
        error: true,
        message: err.message
      })
    );
});

function projectIdValidator(req, res, next) {
  const { id } = req.params;
  projectsDbHelper
    .get(id)
    .then(project => {
      if (project) {
        req.valProject = project;
        next();
      } else
        res.status(404).json({
          err: true,
          message: "Project with that ID not found"
        });
    })
    .catch(err =>
      res.status(500).json({
        error: err.message,
        message: "An error occurred while fetching from database"
      })
    );
}

module.exports = actionRouter;