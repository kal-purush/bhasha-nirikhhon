// utils/errorHandler.ts
export class ServerActionError extends Error {
  public status: number;
  public isOperational: boolean;

  constructor(
    message: string,
    status: number = 500,
    isOperational: boolean = true
  ) {
    super(message);
    this.status = status;
    this.isOperational = isOperational;
    // Maintain proper stack trace for debugging
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }
}

// Helper function
export function handleError(
  error: any,
  defaultMessage: string = "An unexpected error occurred"
) {
  if (process.env.NODE_ENV === "production") {
    console.error(error);
    // return { success: false, error: defaultMessage };
    throw new Error(error);
  } else {
    // Throw the full error in development
    throw error instanceof ServerActionError
      ? error
      : new ServerActionError(error?.message || defaultMessage, 500);
  }
}