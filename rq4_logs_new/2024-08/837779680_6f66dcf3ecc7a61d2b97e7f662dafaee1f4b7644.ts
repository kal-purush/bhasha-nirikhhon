/* eslint-disable max-len */
import Joi from 'joi';

const addressValidation = Joi.object({
  state: Joi.string()
    .max(50)
    .required()
    .messages({
      'any.required': 'The state field is required. Please provide the name of the state where the delivery should be made.',
      'string.base': 'The state must be a text string. Please ensure it is a valid state name.',
      'string.max': 'The state name must not exceed 50 characters. Please provide a shorter state name.',
    }),
  city: Joi.string()
    .max(50)
    .required()
    .messages({
      'any.required': 'The city field is required. Please provide the name of the city where the delivery should be made.',
      'string.base': 'The city must be a text string. Please ensure it is a valid city name.',
      'string.max': 'The city name must not exceed 50 characters. Please provide a shorter city name.',
    }),
  street: Joi.string()
    .max(50)
    .required()
    .messages({
      'any.required': 'The street field is required. Please provide the street name or number for the delivery address.',
      'string.base': 'The street must be a text string. Please ensure it is a valid street name.',
      'string.max': 'The street name must not exceed 50 characters. Please provide a shorter street name.',
    }),
  pin: Joi.string()
    .pattern(/^\d+$/)
    .required()
    .messages({
      'any.required': 'The PIN code is required. Please provide the postal code for the delivery address.',
      'string.pattern.base': 'The PIN code must contain only digits. Please enter a valid numerical postal code.',
    }),
});

const orderItemValidation = Joi.object({
  id: Joi.string()
    .uuid({ version: 'uuidv4' })
    .required()
    .messages({
      'string.guid': 'Product ID must be a valid UUID.',
      'any.required': 'Product ID is required',
      'string.base': 'Product ID must be a string',
    }),
  quantity: Joi.number()
    .integer()
    .min(1)
    .required()
    .messages({
      'number.base': 'Quantity must be an integer number.',
      'number.min': 'Quantity cannot be less than 1.',
      'any.required': 'Quantity is required',
    }),
});

const createOrderValidation = Joi.object({
  items: Joi.array()
    .items(orderItemValidation)
    .min(1)
    .required()
    .messages({
      'array.base': 'Items must be an array.',
      'array.min': 'At least one item is required to create an order.',
      'any.required': 'Items field is required.',
    }),
  address: addressValidation
    .required()
    .messages({
      'any.required': 'Address information is required. Please provide a complete address including state, city, street, and PIN code.',
      'object.base': 'The address must be an object containing state, city, street, and PIN code.',
    }),
  phoneNumber: Joi.string()
    .pattern(/^\+\d{10,15}$/)
    .required()
    .messages({
      'any.required': 'Phone number is required. Please provide a valid phone number.',
      'string.pattern.base': 'Phone number must start with a "+" sign and be followed by 10 to 15 digits. Ensure the number is in the correct international format.',
    }),
  orderOwner: Joi.string()
    .min(3)
    .max(50)
    .required()
    .messages({
      'string.base': 'First name must be a string',
      'string.min': 'First name must be at least 3 characters long',
      'string.max': 'First name must be less than or equal to 50 characters long',
      'any.required': 'First name is required',
    }),
  cardNumber: Joi.string()
    .required()
    .pattern(
      /^(4\d{12}(\d{3})?)|(5[1-5]\d{14})|(2(2[2-9][1-9]\d{12}|2[3-9]\d{13}|[3-6]\d{14}|7[01]\d{13}|720\d{12}))$/,
    )
    .messages({
      'string.base': 'Credit card number must be a string.',
      'string.empty': 'Credit card number is required.',
      'string.pattern.base': 'Invalid credit card number. Only Visa and Mastercard are accepted.',
    }),
});

export { createOrderValidation };