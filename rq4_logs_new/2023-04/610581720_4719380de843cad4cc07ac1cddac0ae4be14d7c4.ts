/**
 * @author WMXPY
 * @namespace Util
 * @description Environment
 */

export type Environment = {

    readonly clientDomain: string;
};

const env: any = (import.meta as any).env;

export const EnvironmentVariables: Environment = {

    clientDomain: env.VITE_CLIENT_DOMAIN,
};