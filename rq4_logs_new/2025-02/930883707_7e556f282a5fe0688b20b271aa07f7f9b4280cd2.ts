export type PseudonymizationDomain = string;

export interface StatusResponse {
    timestamp: string;
    system_id: string;
}

export interface StartSessionResponse {
    session_id: string;
    key_share: string;
}

export interface GetSessionResponse {
    sessions: string[];
}

export interface PseudonymizationResponse {
    encrypted_pseudonym: string;
}

export interface PseudonymizationRequest {
    encrypted_pseudonym: string;
    domain_from: string;
    domain_to: string;
    session_from: string;
    session_to: string;
}

export interface PseudonymizationBatchRequest {
    encrypted_pseudonyms: string[];
    domain_from: string;
    domain_to: string;
    session_from: string;
    session_to: string;
}

export interface PseudonymizationBatchResponse {
    encrypted_pseudonyms: string[];
}