import { z } from 'zod';
import type { CacheClient } from '../api/cacheClient';
import { issueAccessToken } from '../api/issueAccessToken';
import { uploadLgtmImage } from '../api/uploadLgtmImage';
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

export const validateHandleUploadLgtmImageRequestBody = (
  value: unknown
): ValidationResult => {
  const schema = z.object({
    image: z.string().min(1),
    imageExtension: z.enum(acceptedTypesImageExtensions),
  });

  return validation(schema, value);
};

export const handleUploadLgtmImage = async (dto: Dto): Promise<Response> => {
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

  const uploadLgtmImageDto = {
    apiBaseUrl: dto.env.apiBaseUrl,
    accessToken: issueAccessTokenResult.value.jwtAccessToken,
    jsonRequestBody,
  };

  const uploadLgtmImageResult = await uploadLgtmImage(uploadLgtmImageDto);

  const headers: ResponseHeader = {
    'Content-Type': 'application/json',
  };

  if (uploadLgtmImageResult.value.xRequestId != null) {
    headers['X-Request-Id'] = uploadLgtmImageResult.value.xRequestId;
  }

  if (uploadLgtmImageResult.value.xLambdaRequestId != null) {
    headers['X-Lambda-Request-Id'] =
      uploadLgtmImageResult.value.xLambdaRequestId;
  }

  if (isFailureResult(uploadLgtmImageResult)) {
    if (isValidationErrorResponse(uploadLgtmImageResult.value)) {
      return createValidationErrorResponse(
        uploadLgtmImageResult.value.invalidParams,
        headers
      );
    }

    const problemDetails = {
      title: 'failed to upload lgtm image payload too large',
      type: 'PayloadTooLarge',
      status: httpStatusCode.payloadTooLarge,
    } as const;

    return createErrorResponse(
      problemDetails,
      httpStatusCode.payloadTooLarge,
      headers
    );
  }

  const responseBody = {
    createdLgtmImageUrl: uploadLgtmImageResult.value.createdLgtmImageUrl,
  };

  return createSuccessResponse(responseBody, httpStatusCode.accepted, headers);
};