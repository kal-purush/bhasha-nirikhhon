/**
 * Meal plan options for rate plans
 */
export enum MealPlanType {
  NONE = "none",
  BREAKFAST = "breakfast",
  HALF_BOARD = "half_board",
  FULL_BOARD = "full_board",
  ALL_INCLUSIVE = "all_inclusive",
}

/**
 * Duration types for stay requirements
 */
export enum StayDurationType {
  WEEKLY = "weekly",
  MONTHLY = "monthly",
  CUSTOM = "custom",
}

/**
 * Payment method options
 */
export enum PaymentMethod {
  PAY_AT_PROPERTY = "pay_at_property",
  PREPAID = "prepaid",
  PARTIAL_DEPOSIT = "partial_deposit",
}

export enum RatePlanTypes {
  All = "All Types",
  Standard = "Standard",
  DateSpecific = "Date Specific",
  DurationBased = "Duration Based",
}

export enum RatePlanStatus {
  ACTIVE = "Active",
  INACTIVE = "Inactive",
}