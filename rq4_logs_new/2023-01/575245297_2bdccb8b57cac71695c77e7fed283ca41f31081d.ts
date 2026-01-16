import { z } from 'zod';
import type { CacheClient } from '../api/cacheClient';
import { isAcceptableCatImage } from '../api/isAcceptableCatImage';
import { issueAccessToken } from '../api/issueAccessToken';
import { isValidationErrorResponse } from '../api/validationErrorResponse';
import { httpStatusCode } from '../httpStatusCode';
import type { AcceptedTypesImageExtension } from '../lgtmImage';
import { acceptedTypesImageExtensions } from '../lgtmImage';
import { isFailureResult } from '../result';
import { validation, ValidationResult } from '../validator';
import {
  createErrorResponse,
  createSuccessResponse,
  createValidationErrorResponse,
  ResponseHeader,
} from './handlerResponse';

type Dto = {
  env: {
    cognitoTokenEndpoint: string;
    cognitoClientId: string;
    cognitoClientSecret: string;
    apiBaseUrl: string;
    cacheClient: CacheClient;
  };
  requestBody: {
    image: string;
    imageExtension: AcceptedTypesImageExtension;
  };
};

export const validateHandleCatImageValidationRequestBody = (
  value: unknown
): ValidationResult => {
  const schema = z.object({
    image: z.string().min(1),
    imageExtension: z.enum(acceptedTypesImageExtensions),
  });

  return validation(schema, value);
};

export const handleCatImageValidation = async (dto: Dto): Promise<Response> => {
  const issueTokenRequest = {
    endpoint: dto.env.cognitoTokenEndpoint,
    cognitoClientId: dto.env.cognitoClientId,
    cognitoClientSecret: dto.env.cognitoClientSecret,
    cacheClient: dto.env.cacheClient,
  };

  const issueAccessTokenResult = await issueAccessToken(issueTokenRequest);
  if (isFailureResult(issueAccessTokenResult)) {
    const problemDetails = {
      title: 'failed to issue access token',
      type: 'InternalServerError',
      status: httpStatusCode.internalServerError,
    } as const;

    return createErrorResponse(
      problemDetails,
      httpStatusCode.internalServerError
    );
  }

  const jsonRequestBody = JSON.stringify(dto.requestBody);

  const isAcceptableCatImageDto = {
    apiBaseUrl: dto.env.apiBaseUrl,
    accessToken: issueAccessTokenResult.value.jwtAccessToken,
    jsonRequestBody,
  };

  const isAcceptableCatImageResult = await isAcceptableCatImage(
    isAcceptableCatImageDto
  );

  const headers: ResponseHeader = {
    'Content-Type': 'application/json',
  };

  if (isAcceptableCatImageResult.value.xRequestId != null) {
    headers['X-Request-Id'] = isAcceptableCatImageResult.value.xRequestId;
  }

  if (isAcceptableCatImageResult.value.xLambdaRequestId != null) {
    headers['X-Lambda-Request-Id'] =
      isAcceptableCatImageResult.value.xLambdaRequestId;
  }

  if (isFailureResult(isAcceptableCatImageResult)) {
    if (isValidationErrorResponse(isAcceptableCatImageResult.value)) {
      return createValidationErrorResponse(
        isAcceptableCatImageResult.value.invalidParams,
        headers
      );
    }

    const problemDetails = {
      title: 'failed to is acceptable cat image',
      type: 'InternalServerError',
      status: httpStatusCode.internalServerError,
    } as const;

    return createErrorResponse(
      problemDetails,
      httpStatusCode.internalServerError,
      headers
    );
  }

  const responseBody =
    isAcceptableCatImageResult.value.isAcceptableCatImageResponse;

  return createSuccessResponse(responseBody, httpStatusCode.ok, headers);
};