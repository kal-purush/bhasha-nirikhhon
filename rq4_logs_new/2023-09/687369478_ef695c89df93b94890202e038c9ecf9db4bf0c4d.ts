import { checkSchema } from "express-validator";

const validateUser = checkSchema({
  username: {
    exists: {
      errorMessage: "Username is required",
      options: { checkFalsy: true },
    },
    isString: { errorMessage: "Username should be a string" },
  },
  displayName: {
    exists: {
      errorMessage: "Display name is required",
      options: { checkFalsy: true },
    },
    isString: { errorMessage: "Display name should be a string" },
  },
  password: {
    exists: {
      errorMessage: "Password is required",
      options: { checkFalsy: true },
    },
    isString: { errorMessage: "Password should be a string" },
  },
  role: {
    isString: { errorMessage: "Role should be a string" },
    isIn: {
      options: [["Basic", "Admin"]],
      errorMessage: "Role is invalid",
    },
  },
});

export default validateUser;