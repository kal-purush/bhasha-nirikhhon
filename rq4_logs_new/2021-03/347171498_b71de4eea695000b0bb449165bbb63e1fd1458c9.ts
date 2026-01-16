import { GraphQLFormattedError, formatError } from 'graphql';
import { isServerError } from '@core/error/base.error';

interface FormatedServerError extends GraphQLFormattedError {
  code?: number;
  name?: string;
  additionalInfo?: string;
}

export const errorFormatter = (error: any) => {
  let data: FormatedServerError = formatError(error);
  const { originalError } = error;

  if (isServerError(error.originalError)) {
    data = {
      ...data,
      code: originalError.code,
      name: originalError.name,
      message: originalError.message,
      additionalInfo: originalError.additionalInfo,
    };
  }

  return data;
};