export interface Params {
  data: string;
  dir: string;
}

export function validate(param: Params) {
  if (Object.prototype.toString.call(param) !== "[object Object]") {
    throw new Error("the param must be object");
  } else {
    const { data, dir } = param;
    if (!data) {
      throw new Error("Property 'data' is missing ");
    } else if (typeof data !== "string") {
      throw new Error("Property 'data' Type is string ");
    }

    if (!dir) {
      throw new Error("Property 'dir' is missing ");
    } else if (typeof dir !== "string") {
      throw new Error("Property 'dir' Type is string ");
    }
  }
}