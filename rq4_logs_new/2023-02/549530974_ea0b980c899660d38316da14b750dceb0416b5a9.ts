import * as yup from 'yup';

export const productFormSchema = yup.object({
  name: yup.string().required('Name is required').max(120, 'Name cannot be longer than 120 characters'),
  desc: yup.string().optional().max(500, 'Description cannot be longer than 500 characters'),
  price: yup.number().min(0, 'Price must be positive'),
  category: yup.string().optional(),
  stock: yup.number().required('Stock number is required').min(0).integer(),
  tags: yup.array().of(yup.number()).required('Tag is required').min(1, 'At least one tag is required'),
});

export type ProductFormSchema = yup.InferType<typeof productFormSchema>;