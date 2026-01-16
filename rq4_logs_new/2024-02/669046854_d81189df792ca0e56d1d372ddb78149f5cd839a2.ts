import { productValidationRules } from "@/validation/zod/product-add-form";
import toast from "react-hot-toast";
import { z } from "zod";

// Define the types for the form values and errors
type Values = z.infer<typeof productValidationRules>;
type Errors = Partial<Record<keyof Values, string>>;

/**validate product form */
export async function productValidate(values: any): Promise<Errors> {
  let errors: Errors = {};

  // Validate the form values
  const result = productValidationRules.safeParse(values);
  if (!result.success) {
    result.error.errors.forEach((error) => {
      const message = error.message;
      const path = error.path[0] as keyof Values;
      errors[path] = toast.error(message);
    });
  }

  return errors;
}