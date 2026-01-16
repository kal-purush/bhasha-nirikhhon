import type { CacheClient } from '../api/cacheClient';
import { fetchLgtmImagesInRecentlyCreated } from '../api/fetchLgtmImages';
import { issueAccessToken } from '../api/issueAccessToken';
import { isValidationErrorResponse } from '../api/validationErrorResponse';
import { httpStatusCode } from '../httpStatusCode';
import { isFailureResult } from '../result';
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
};

export const handleFetchLgtmImagesInRecentlyCreated = async (
  dto: Dto
): Promise<Response> => {
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

  const fetchLgtmImagesRequest = {
    apiBaseUrl: dto.env.apiBaseUrl,
    accessToken: issueAccessTokenResult.value.jwtAccessToken,
  };

  const fetchLgtmImagesResult = await fetchLgtmImagesInRecentlyCreated(
    fetchLgtmImagesRequest
  );

  const headers: ResponseHeader = {
    'Content-Type': 'application/json',
  };

  if (fetchLgtmImagesResult.value.xRequestId != null) {
    headers['X-Request-Id'] = fetchLgtmImagesResult.value.xRequestId;
  }

  if (fetchLgtmImagesResult.value.xLambdaRequestId != null) {
    headers['X-Lambda-Request-Id'] =
      fetchLgtmImagesResult.value.xLambdaRequestId;
  }

  if (isFailureResult(fetchLgtmImagesResult)) {
    if (isValidationErrorResponse(fetchLgtmImagesResult.value)) {
      return createValidationErrorResponse(
        fetchLgtmImagesResult.value.invalidParams,
        headers
      );
    }

    const problemDetails = {
      title: 'failed to fetch lgtm images in random',
      type: 'InternalServerError',
      status: httpStatusCode.internalServerError,
    } as const;

    return createErrorResponse(
      problemDetails,
      httpStatusCode.internalServerError
    );
  }

  const responseBody = { lgtmImages: fetchLgtmImagesResult.value.lgtmImages };

  return createSuccessResponse(responseBody, httpStatusCode.ok, headers);
};