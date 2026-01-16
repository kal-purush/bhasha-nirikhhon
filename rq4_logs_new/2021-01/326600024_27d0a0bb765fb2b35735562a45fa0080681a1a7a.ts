class MissingMemberInformationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MISSING_MEMBER_INFORMATION';
  }
}

class ExistingMemberError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EXISTING_MEMBER';
  }
}

class MemberNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MEMBER_NOT_FOUND';
  }
}

class MissingVendorInformationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MISSING_VENDOR_INFORMATION';
  }
}

class ExistingVendorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EXISTING_VENDOR';
  }
}

class VendorNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'VENDOR_NOT_FOUND';
  }
}

class MissingPromoInformationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MISSING_PROMO_INFORMATION';
  }
}

class InvalidPromoTemplateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'INVALID_PROMO_TEMPLATE';
  }
}

class InvalidPromoRequiredMemberFieldError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'INVALID_PROMO_REQUIRED_MEMBER_FIELD';
  }
}

class InvalidPromoInformationGivenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'INVALID_PROMO_INFORMATION_GIVEN';
  }
}

class PromoNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PROMO_NOT_FOUND';
  }
}

class ActivePromoError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ACTIVE_PROMO';
  }
}

class InvalidPromoError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'INVALID_PROMO';
  }
}

class ExistingEnrollmentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EXISTING_ENROLLMENT';
  }
}

class RequiredMemberFieldsNotMetError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'REQUIRED_MEMBER_FIELDS_NOT_MET';
  }
}

class NotEnoughBalanceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NOT_ENOUGH_BALANCE';
  }
}

class PromoEnrollmentRequestNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PROMO_ENROLLMENT_REQUEST_NOT_FOUND';
  }
}

class MissingCredentialsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MISSING_CREDENTIALS';
  }
}

class InvalidCredentialsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'INVALID_CREDENTIALS';
  }
}

class MissingPromoEnrollmentRequestInformationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MISSING_PROMO_ENROLLMENT_REQUEST_INFORMATION_ERROR';
  }
}

class NotAllowedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NOT_ALLOWED_ERROR';
  }
}

class DatabaseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DATABASE_ERROR';
  }
}

export {
  MissingMemberInformationError,
  ExistingMemberError,
  MemberNotFoundError,
  MissingVendorInformationError,
  ExistingVendorError,
  VendorNotFoundError,
  MissingPromoInformationError,
  InvalidPromoTemplateError,
  InvalidPromoRequiredMemberFieldError,
  InvalidPromoInformationGivenError,
  PromoNotFoundError,
  ActivePromoError,
  InvalidPromoError,
  ExistingEnrollmentError,
  RequiredMemberFieldsNotMetError,
  NotEnoughBalanceError,
  PromoEnrollmentRequestNotFoundError,
  MissingCredentialsError,
  InvalidCredentialsError,
  MissingPromoEnrollmentRequestInformationError,
  NotAllowedError,
  DatabaseError,
};