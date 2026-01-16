import { Request } from 'express';
import { ExtendedRequest } from '../types/incoming-message';
export declare class BasicStrategy {
    name?: string;
    authenticate(req: Request | ExtendedRequest, options?: any): void;
    success?: Strategy['success'];
    fail?: Strategy['fail'];
    redirect?: Strategy['redirect'];
    pass?: Strategy['pass'];
    error?: Strategy['error'];
}
export interface Strategy {
    name: string;
    _deserializeUser?: Function;
    authenticate(req: ExtendedRequest, options?: any): void;
    success(user: any, info?: any): void;
    fail(challenge: any, status: number): void;
    fail(status: number): void;
    redirect(url: string, status?: number): void;
    pass(): void;
    error(err: Error): void;
}
export default BasicStrategy;