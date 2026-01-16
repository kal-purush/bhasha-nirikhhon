/// <reference path="src/custom.d.ts" />
import { Configuration } from "./configuration";
/**
 *
 * @export
 */
export declare const COLLECTION_FORMATS: {
    csv: string;
    ssv: string;
    tsv: string;
    pipes: string;
};
/**
 *
 * @export
 * @interface FetchAPI
 */
export interface FetchAPI {
    (url: string, init?: any): Promise<Response>;
}
/**
 *
 * @export
 * @interface FetchArgs
 */
export interface FetchArgs {
    url: string;
    options: any;
}
/**
 *
 * @export
 * @class BaseAPI
 */
export declare class BaseAPI {
    protected basePath: string;
    protected fetch: FetchAPI;
    protected configuration: Configuration;
    constructor(configuration?: Configuration, basePath?: string, fetch?: FetchAPI);
}
/**
 *
 * @export
 * @class RequiredError
 * @extends {Error}
 */
export declare class RequiredError extends Error {
    field: string;
    name: "RequiredError";
    constructor(field: string, msg?: string);
}
/**
 *
 * @export
 * @interface MainAuth
 */
export interface MainAuth {
    /**
     *
     * @type {string}
     * @memberof MainAuth
     */
    created_at?: string;
    /**
     *
     * @type {number}
     * @memberof MainAuth
     */
    expires_at?: number;
    /**
     *
     * @type {number}
     * @memberof MainAuth
     */
    id?: number;
    /**
     *
     * @type {string}
     * @memberof MainAuth
     */
    token?: string;
    /**
     *
     * @type {string}
     * @memberof MainAuth
     */
    updated_at?: string;
    /**
     *
     * @type {MainUser}
     * @memberof MainAuth
     */
    user?: MainUser;
    /**
     *
     * @type {number}
     * @memberof MainAuth
     */
    user_id?: number;
}
/**
 *
 * @export
 * @interface MainAvailableResponse
 */
export interface MainAvailableResponse {
    /**
     *
     * @type {boolean}
     * @memberof MainAvailableResponse
     */
    is_available?: boolean;
}
/**
 *
 * @export
 * @interface MainGenericResponse
 */
export interface MainGenericResponse {
    /**
     *
     * @type {string}
     * @memberof MainGenericResponse
     */
    error?: string;
    /**
     *
     * @type {boolean}
     * @memberof MainGenericResponse
     */
    success?: boolean;
}
/**
 *
 * @export
 * @interface MainUnit
 */
export interface MainUnit {
    /**
     *
     * @type {string}
     * @memberof MainUnit
     */
    created_at?: string;
    /**
     *
     * @type {string}
     * @memberof MainUnit
     */
    editor_type?: string;
    /**
     *
     * @type {string}
     * @memberof MainUnit
     */
    file_name?: string;
    /**
     *
     * @type {number}
     * @memberof MainUnit
     */
    id?: number;
    /**
     *
     * @type {number}
     * @memberof MainUnit
     */
    loc_deleted?: number;
    /**
     *
     * @type {number}
     * @memberof MainUnit
     */
    loc_written?: number;
    /**
     *
     * @type {string}
     * @memberof MainUnit
     */
    os?: string;
    /**
     *
     * @type {string}
     * @memberof MainUnit
     */
    project_name?: string;
    /**
     *
     * @type {string}
     * @memberof MainUnit
     */
    started_at?: string;
    /**
     *
     * @type {string}
     * @memberof MainUnit
     */
    stopped_at?: string;
    /**
     *
     * @type {string}
     * @memberof MainUnit
     */
    updated_at?: string;
    /**
     *
     * @type {number}
     * @memberof MainUnit
     */
    user_id?: number;
}
/**
 *
 * @export
 * @interface MainUser
 */
export interface MainUser {
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    created_at?: string;
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    editor_token?: string;
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    email: string;
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    first_name: string;
    /**
     *
     * @type {number}
     * @memberof MainUser
     */
    id?: number;
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    last_name: string;
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    password: string;
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    profile_image?: string;
    /**
     *
     * @type {Array<MainUnit>}
     * @memberof MainUser
     */
    units?: Array<MainUnit>;
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    updated_at?: string;
    /**
     *
     * @type {string}
     * @memberof MainUser
     */
    username: string;
}
/**
 *
 * @export
 * @interface MainVersionResponse
 */
export interface MainVersionResponse {
    /**
     *
     * @type {string}
     * @memberof MainVersionResponse
     */
    download?: string;
    /**
     *
     * @type {boolean}
     * @memberof MainVersionResponse
     */
    latest?: boolean;
}
/**
 * AuthApi - fetch parameter creator
 * @export
 */
export declare const AuthApiFetchParamCreator: (configuration?: Configuration) => {
    /**
     * Authenticates a user and returns a JWT on successful login
     * @summary Authenticates a user
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    login(options?: any): FetchArgs;
    /**
     * Sends an email to the user with a link to reset their password
     * @summary Starts a password reset
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    passwordReset(options?: any): FetchArgs;
    /**
     * Allows the user to reset their password with the submitted password
     * @summary Resets a user's password
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    resetPassword(options?: any): FetchArgs;
};
/**
 * AuthApi - functional programming interface
 * @export
 */
export declare const AuthApiFp: (configuration?: Configuration) => {
    /**
     * Authenticates a user and returns a JWT on successful login
     * @summary Authenticates a user
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    login(options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainAuth>;
    /**
     * Sends an email to the user with a link to reset their password
     * @summary Starts a password reset
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    passwordReset(options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainGenericResponse>;
    /**
     * Allows the user to reset their password with the submitted password
     * @summary Resets a user's password
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    resetPassword(options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainGenericResponse>;
};
/**
 * AuthApi - factory interface
 * @export
 */
export declare const AuthApiFactory: (configuration?: Configuration, fetch?: FetchAPI, basePath?: string) => {
    /**
     * Authenticates a user and returns a JWT on successful login
     * @summary Authenticates a user
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    login(options?: any): Promise<MainAuth>;
    /**
     * Sends an email to the user with a link to reset their password
     * @summary Starts a password reset
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    passwordReset(options?: any): Promise<MainGenericResponse>;
    /**
     * Allows the user to reset their password with the submitted password
     * @summary Resets a user's password
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    resetPassword(options?: any): Promise<MainGenericResponse>;
};
/**
 * AuthApi - object-oriented interface
 * @export
 * @class AuthApi
 * @extends {BaseAPI}
 */
export declare class AuthApi extends BaseAPI {
    /**
     * Authenticates a user and returns a JWT on successful login
     * @summary Authenticates a user
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof AuthApi
     */
    login(options?: any): Promise<MainAuth>;
    /**
     * Sends an email to the user with a link to reset their password
     * @summary Starts a password reset
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof AuthApi
     */
    passwordReset(options?: any): Promise<MainGenericResponse>;
    /**
     * Allows the user to reset their password with the submitted password
     * @summary Resets a user's password
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof AuthApi
     */
    resetPassword(options?: any): Promise<MainGenericResponse>;
}
/**
 * CoreApi - fetch parameter creator
 * @export
 */
export declare const CoreApiFetchParamCreator: (configuration?: Configuration) => {
    /**
     * This endpoint takes a few parameters and with those parameters, it looks to see if
     * @summary Returns a link of the latest version of the Core app
     * @param {string} X_Hackerlog_EditorToken X-Hackerlog-EditorToken
     * @param {string} currentVersion Current core version in client
     * @param {string} os The client OS
     * @param {string} arch The client architecture
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    coreVersionGet(X_Hackerlog_EditorToken: string, currentVersion: string, os: string, arch: string, options?: any): FetchArgs;
};
/**
 * CoreApi - functional programming interface
 * @export
 */
export declare const CoreApiFp: (configuration?: Configuration) => {
    /**
     * This endpoint takes a few parameters and with those parameters, it looks to see if
     * @summary Returns a link of the latest version of the Core app
     * @param {string} X_Hackerlog_EditorToken X-Hackerlog-EditorToken
     * @param {string} currentVersion Current core version in client
     * @param {string} os The client OS
     * @param {string} arch The client architecture
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    coreVersionGet(X_Hackerlog_EditorToken: string, currentVersion: string, os: string, arch: string, options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainVersionResponse>;
};
/**
 * CoreApi - factory interface
 * @export
 */
export declare const CoreApiFactory: (configuration?: Configuration, fetch?: FetchAPI, basePath?: string) => {
    /**
     * This endpoint takes a few parameters and with those parameters, it looks to see if
     * @summary Returns a link of the latest version of the Core app
     * @param {string} X_Hackerlog_EditorToken X-Hackerlog-EditorToken
     * @param {string} currentVersion Current core version in client
     * @param {string} os The client OS
     * @param {string} arch The client architecture
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    coreVersionGet(X_Hackerlog_EditorToken: string, currentVersion: string, os: string, arch: string, options?: any): Promise<MainVersionResponse>;
};
/**
 * CoreApi - object-oriented interface
 * @export
 * @class CoreApi
 * @extends {BaseAPI}
 */
export declare class CoreApi extends BaseAPI {
    /**
     * This endpoint takes a few parameters and with those parameters, it looks to see if
     * @summary Returns a link of the latest version of the Core app
     * @param {string} X_Hackerlog_EditorToken X-Hackerlog-EditorToken
     * @param {string} currentVersion Current core version in client
     * @param {string} os The client OS
     * @param {string} arch The client architecture
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof CoreApi
     */
    coreVersionGet(X_Hackerlog_EditorToken: string, currentVersion: string, os: string, arch: string, options?: any): Promise<MainVersionResponse>;
}
/**
 * MailingListApi - fetch parameter creator
 * @export
 */
export declare const MailingListApiFetchParamCreator: (configuration?: Configuration) => {
    /**
     * This adds a user to the mailing list
     * @summary Adds a user to the mailing list
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    listPost(options?: any): FetchArgs;
};
/**
 * MailingListApi - functional programming interface
 * @export
 */
export declare const MailingListApiFp: (configuration?: Configuration) => {
    /**
     * This adds a user to the mailing list
     * @summary Adds a user to the mailing list
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    listPost(options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainGenericResponse>;
};
/**
 * MailingListApi - factory interface
 * @export
 */
export declare const MailingListApiFactory: (configuration?: Configuration, fetch?: FetchAPI, basePath?: string) => {
    /**
     * This adds a user to the mailing list
     * @summary Adds a user to the mailing list
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    listPost(options?: any): Promise<MainGenericResponse>;
};
/**
 * MailingListApi - object-oriented interface
 * @export
 * @class MailingListApi
 * @extends {BaseAPI}
 */
export declare class MailingListApi extends BaseAPI {
    /**
     * This adds a user to the mailing list
     * @summary Adds a user to the mailing list
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof MailingListApi
     */
    listPost(options?: any): Promise<MainGenericResponse>;
}
/**
 * UnitsApi - fetch parameter creator
 * @export
 */
export declare const UnitsApiFetchParamCreator: (configuration?: Configuration) => {
    /**
     * This gets all of the units of work for a specific user. The user is identified by the
     * @summary Gets units of work for a user
     * @param {string} X_Hackerlog_EditorToken X-Hackerlog-EditorToken
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    unitsGet(X_Hackerlog_EditorToken: string, options?: any): FetchArgs;
};
/**
 * UnitsApi - functional programming interface
 * @export
 */
export declare const UnitsApiFp: (configuration?: Configuration) => {
    /**
     * This gets all of the units of work for a specific user. The user is identified by the
     * @summary Gets units of work for a user
     * @param {string} X_Hackerlog_EditorToken X-Hackerlog-EditorToken
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    unitsGet(X_Hackerlog_EditorToken: string, options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainUnit[]>;
};
/**
 * UnitsApi - factory interface
 * @export
 */
export declare const UnitsApiFactory: (configuration?: Configuration, fetch?: FetchAPI, basePath?: string) => {
    /**
     * This gets all of the units of work for a specific user. The user is identified by the
     * @summary Gets units of work for a user
     * @param {string} X_Hackerlog_EditorToken X-Hackerlog-EditorToken
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    unitsGet(X_Hackerlog_EditorToken: string, options?: any): Promise<MainUnit[]>;
};
/**
 * UnitsApi - object-oriented interface
 * @export
 * @class UnitsApi
 * @extends {BaseAPI}
 */
export declare class UnitsApi extends BaseAPI {
    /**
     * This gets all of the units of work for a specific user. The user is identified by the
     * @summary Gets units of work for a user
     * @param {string} X_Hackerlog_EditorToken X-Hackerlog-EditorToken
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof UnitsApi
     */
    unitsGet(X_Hackerlog_EditorToken: string, options?: any): Promise<MainUnit[]>;
}
/**
 * UsersApi - fetch parameter creator
 * @export
 */
export declare const UsersApiFetchParamCreator: (configuration?: Configuration) => {
    /**
     * Adds a profile image to a user
     * @summary Add Profile Image
     * @param {string} id The ID of the user
     * @param {any} image_url The URL of the profile image
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    addProfileImage(id: string, image_url: any, options?: any): FetchArgs;
    /**
     * Creates a user with the body params that are passed in
     * @summary Creates/Registers a user
     * @param {MainUser} user User object: first_name, last_name, email, password, username
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    createUser(user: MainUser, options?: any): FetchArgs;
    /**
     * Finds a user given their ID as a path param
     * @summary Gets a user by their ID
     * @param {number} id User ID
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    findUser(id: number, options?: any): FetchArgs;
    /**
     * Finds a user given their editor token as a path param
     * @summary Gets a user by their editor token
     * @param {any} editor User&#39;s Editor Token
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    fundUserByEditorToken(editor: any, options?: any): FetchArgs;
    /**
     * Checks if an email is available and responds as such
     * @summary Checks if an email is available
     * @param {string} [q] email search using q as key
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    isEmailAvailable(q?: string, options?: any): FetchArgs;
    /**
     * Checks if a username is available and responds as such
     * @summary Checks if a username is available
     * @param {string} [q] Username search using q as key
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    isUsernameAvailable(q?: string, options?: any): FetchArgs;
};
/**
 * UsersApi - functional programming interface
 * @export
 */
export declare const UsersApiFp: (configuration?: Configuration) => {
    /**
     * Adds a profile image to a user
     * @summary Add Profile Image
     * @param {string} id The ID of the user
     * @param {any} image_url The URL of the profile image
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    addProfileImage(id: string, image_url: any, options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainGenericResponse>;
    /**
     * Creates a user with the body params that are passed in
     * @summary Creates/Registers a user
     * @param {MainUser} user User object: first_name, last_name, email, password, username
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    createUser(user: MainUser, options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainUser>;
    /**
     * Finds a user given their ID as a path param
     * @summary Gets a user by their ID
     * @param {number} id User ID
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    findUser(id: number, options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainUser>;
    /**
     * Finds a user given their editor token as a path param
     * @summary Gets a user by their editor token
     * @param {any} editor User&#39;s Editor Token
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    fundUserByEditorToken(editor: any, options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainUser>;
    /**
     * Checks if an email is available and responds as such
     * @summary Checks if an email is available
     * @param {string} [q] email search using q as key
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    isEmailAvailable(q?: string, options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainAvailableResponse>;
    /**
     * Checks if a username is available and responds as such
     * @summary Checks if a username is available
     * @param {string} [q] Username search using q as key
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    isUsernameAvailable(q?: string, options?: any): (fetch?: FetchAPI, basePath?: string) => Promise<MainAvailableResponse>;
};
/**
 * UsersApi - factory interface
 * @export
 */
export declare const UsersApiFactory: (configuration?: Configuration, fetch?: FetchAPI, basePath?: string) => {
    /**
     * Adds a profile image to a user
     * @summary Add Profile Image
     * @param {string} id The ID of the user
     * @param {any} image_url The URL of the profile image
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    addProfileImage(id: string, image_url: any, options?: any): Promise<MainGenericResponse>;
    /**
     * Creates a user with the body params that are passed in
     * @summary Creates/Registers a user
     * @param {MainUser} user User object: first_name, last_name, email, password, username
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    createUser(user: MainUser, options?: any): Promise<MainUser>;
    /**
     * Finds a user given their ID as a path param
     * @summary Gets a user by their ID
     * @param {number} id User ID
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    findUser(id: number, options?: any): Promise<MainUser>;
    /**
     * Finds a user given their editor token as a path param
     * @summary Gets a user by their editor token
     * @param {any} editor User&#39;s Editor Token
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    fundUserByEditorToken(editor: any, options?: any): Promise<MainUser>;
    /**
     * Checks if an email is available and responds as such
     * @summary Checks if an email is available
     * @param {string} [q] email search using q as key
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    isEmailAvailable(q?: string, options?: any): Promise<MainAvailableResponse>;
    /**
     * Checks if a username is available and responds as such
     * @summary Checks if a username is available
     * @param {string} [q] Username search using q as key
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     */
    isUsernameAvailable(q?: string, options?: any): Promise<MainAvailableResponse>;
};
/**
 * UsersApi - object-oriented interface
 * @export
 * @class UsersApi
 * @extends {BaseAPI}
 */
export declare class UsersApi extends BaseAPI {
    /**
     * Adds a profile image to a user
     * @summary Add Profile Image
     * @param {string} id The ID of the user
     * @param {any} image_url The URL of the profile image
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof UsersApi
     */
    addProfileImage(id: string, image_url: any, options?: any): Promise<MainGenericResponse>;
    /**
     * Creates a user with the body params that are passed in
     * @summary Creates/Registers a user
     * @param {MainUser} user User object: first_name, last_name, email, password, username
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof UsersApi
     */
    createUser(user: MainUser, options?: any): Promise<MainUser>;
    /**
     * Finds a user given their ID as a path param
     * @summary Gets a user by their ID
     * @param {number} id User ID
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof UsersApi
     */
    findUser(id: number, options?: any): Promise<MainUser>;
    /**
     * Finds a user given their editor token as a path param
     * @summary Gets a user by their editor token
     * @param {any} editor User&#39;s Editor Token
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof UsersApi
     */
    fundUserByEditorToken(editor: any, options?: any): Promise<MainUser>;
    /**
     * Checks if an email is available and responds as such
     * @summary Checks if an email is available
     * @param {string} [q] email search using q as key
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof UsersApi
     */
    isEmailAvailable(q?: string, options?: any): Promise<MainAvailableResponse>;
    /**
     * Checks if a username is available and responds as such
     * @summary Checks if a username is available
     * @param {string} [q] Username search using q as key
     * @param {*} [options] Override http request option.
     * @throws {RequiredError}
     * @memberof UsersApi
     */
    isUsernameAvailable(q?: string, options?: any): Promise<MainAvailableResponse>;
}