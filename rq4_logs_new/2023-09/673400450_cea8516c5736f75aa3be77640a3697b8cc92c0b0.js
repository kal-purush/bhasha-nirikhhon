const { check } = require("express-validator");
const {validatorMiddleware} = require("../../middlewares/validator-middleware");

exports.addProfessorValidator = [
    check("name")
      .notEmpty()
      .withMessage("Name is required"),
    check("email")
      .notEmpty()
      .withMessage("Email is required")
      .isEmail()
      .withMessage("Invalid email address"),
    check("password")
      .notEmpty()
      .withMessage("Password is required")
      .isLength({ min: 8 })
      .withMessage("Password must be at least 8 characters"),
    check("collageId")
      .notEmpty()
      .withMessage("Collage ID is required"),
    check("natId")
      .notEmpty()
      .withMessage("National ID is required"),
    
    validatorMiddleware
  ];