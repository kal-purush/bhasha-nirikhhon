import { ApiError } from "@point-hub/express-error-handler";
import Validator from "validatorjs";
import Validatorjs from "validatorjs";

Validator.register("unique", function(value, requirement, attribute) {
  const values = Object.assign([], value);
  const uniqueData = Array.from(
    new Set(values.map((row) => row[requirement])),
  );
  const isUnique = values.length === uniqueData.length;
  if (isUnique) {
    return true;
  }
  return false;
}, 'Please provide a unique :attribute number.');

export const validate = (body: any) => {
  const validation = new Validatorjs(body, {
    name: "required",
    branch: "required",
    code: "required",
    notes: "required",
    accounts: "required|array|unique:number",
    "accounts.*.number": "required|numeric" 
  });
 
  if (validation.fails()) {
    throw new ApiError(422, validation.errors.errors);
  }
};