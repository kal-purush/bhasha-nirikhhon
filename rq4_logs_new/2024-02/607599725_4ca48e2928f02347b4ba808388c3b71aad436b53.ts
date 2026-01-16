import { ProblemDetails } from '../models/problem-details';
import { MonoCloudRequestException } from './monocloud-request-exception';

/// <summary>
/// The MonoCloud Bad Request Exception
/// </summary>
export class MonoCloudBadRequestException extends MonoCloudRequestException {
  /// <summary>
  /// Initializes the MonoCloudBadRequestException Class
  /// </summary>
  /// <param name="response">The problem details returned from the server.</param>
  constructor(response: ProblemDetails);

  /// <summary>
  /// Initializes the MonoCloudBadRequestException Class
  /// </summary>
  /// <param name="message">The error message.</param>
  constructor(message: string);
  constructor(arg: ProblemDetails | string) {
    if (typeof arg === 'string') {
      super(arg);
    } else {
      super(arg);
    }
  }
}