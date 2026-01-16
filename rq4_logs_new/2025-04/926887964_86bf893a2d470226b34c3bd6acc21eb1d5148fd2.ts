/**
 * Common Error Format per system specifications
 * See: wms-main/architectural intent/common formats/error/Common Error Format.md
 *
 * Only criticality, id, and detail fields are required.
 * All other fields are optional and should only be included when needed.
 */
export interface CommonErrorType {
  // Required fields
  criticality: 'critical' | 'non-critical' | 'unknown';
  id: string;
  detail: string;

  // Optional fields
  title?: string;
  recoverySuggestion?: string;
  criticalityExplanation?: string;
  errorCode?: string;
  suggestedHTTPCode?: number;
  recoveryData?: Record<string, unknown>;
  otherErrors?: CommonErrorType[];
}