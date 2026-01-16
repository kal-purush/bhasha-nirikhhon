import { SetMetadata } from '@nestjs/common';

export const API_VERSION = 'API_VERSION';
export const ApiVersion = (version: string) => SetMetadata(API_VERSION, version);