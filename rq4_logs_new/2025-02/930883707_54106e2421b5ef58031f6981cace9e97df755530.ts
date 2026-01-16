import { SystemId } from "./sessions.js";

/**
 * Type for a token callback function that returns a Promise with an auth token
 */
export type TokenCallback = () => Promise<string>;

/**
 * Interface for authentication methods
 */
export interface Auth {
    tokenType(): string;
    token(): Promise<string>;
}

/**
 * Basic bearer token authentication using a static token
 */
export class BearerTokenAuth implements Auth {
    private tokenValue: string;

    /**
     * Create a new BearerTokenAuth with a static token
     */
    constructor(token: string) {
        this.tokenValue = token;
    }

    /**
     * Get the token type (Bearer)
     */
    tokenType(): string {
        return "Bearer";
    }

    /**
     * Get the token
     */
    async token(): Promise<string> {
        return this.tokenValue;
    }
}

/**
 * Auth implementation that uses a callback for token retrieval
 */
export class CallbackAuth implements Auth {
    private tokenCallback: TokenCallback;
    private _tokenType: string;

    /**
     * Create a new CallbackAuth with a token callback
     * @param tokenCallback Function that returns a Promise resolving to a token
     * @param tokenType The token type to use in Authorization header (defaults to "Bearer")
     */
    constructor(tokenCallback: TokenCallback, tokenType: string = "Bearer") {
        this.tokenCallback = tokenCallback;
        this._tokenType = tokenType;
    }

    /**
     * Get the token type
     */
    tokenType(): string {
        return this._tokenType;
    }

    /**
     * Get the token by calling the callback
     */
    async token(): Promise<string> {
        return this.tokenCallback();
    }
}

/**
 * Authentication token alias
 */
export type AuthToken = string;

/**
 * Collection of authentication tokens for multiple systems
 */
export class SystemAuths {
    private auths = new Map<SystemId, Auth>();

    /**
     * Create a new SystemAuths with a map of system IDs to Auth implementations
     */
    constructor(auths: Map<SystemId, Auth>) {
        this.auths = auths;
    }

    /**
     * Create a SystemAuths from a map of system IDs to raw auth tokens
     */
    static fromTokens(tokens: Map<SystemId, string>): SystemAuths {
        const auths = new Map<SystemId, Auth>();

        for (const [systemId, token] of tokens.entries()) {
            auths.set(systemId, new BearerTokenAuth(token));
        }

        return new SystemAuths(auths);
    }

    /**
     * Create a SystemAuths from a map of system IDs to token callbacks
     */
    static fromCallbacks(callbacks: Map<SystemId, TokenCallback>): SystemAuths {
        const auths = new Map<SystemId, Auth>();

        for (const [systemId, callback] of callbacks.entries()) {
            auths.set(systemId, new CallbackAuth(callback));
        }

        return new SystemAuths(auths);
    }

    /**
     * Get the Auth for a system
     */
    get(systemId: SystemId): Auth | undefined {
        return this.auths.get(systemId);
    }
}