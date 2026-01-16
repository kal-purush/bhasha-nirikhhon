export const ProposalErrors = {
  NOT_FOUND: "Proposal not found",
  UNAUTHORIZED: "Not authorized to delete this proposal",
  DRAFT_ONLY: "Only draft proposals can be deleted",
  GENERAL_ERROR: "Failed to process proposal operation",
} as const;

export const AuthErrors = {
  UNAUTHORIZED: "Please log in to continue",
  FORBIDDEN: "You don't have permission to perform this action",
} as const;

export const HTTPStatus = {
  OK: 200,
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  INTERNAL_ERROR: 500,
} as const;

export type ProposalErrorType = typeof ProposalErrors[keyof typeof ProposalErrors];
export type AuthErrorType = typeof AuthErrors[keyof typeof AuthErrors]; 