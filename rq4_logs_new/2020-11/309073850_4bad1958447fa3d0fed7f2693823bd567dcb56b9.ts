import * as Joi from "joi";

export interface Product {
  title: string;
  description: string;
  price: number;
  count: number;
}

const productSchema = Joi.object({
  title: Joi.string().min(3).max(50).required(),
  description: Joi.string().min(3).max(100).required(),
  price: Joi.number().required(),
  count: Joi.number().integer().required(),
});

export const validateProduct = (product: Product): { errors: string[] } | null => {
  const validation = productSchema.validate(product, { abortEarly: false });
  if (validation.error) {
    const errors = validation.error.details.map(({ message }) => message);
    return { errors };
  }
  return null;
};