import { BadRequestException, Inject, Injectable } from '@nestjs/common';

import { AxiosInstance } from 'axios';
import axios from 'axios';
import {
  ABSTRACT_API,
  EMAIL_VALIDATE_PUBLIC_API,
} from '../utils/validation.constant';
import { TracingLoggerService } from '../../../logger/tracing-logger.service';

@Injectable()
export class EmailValidationHelper {
  private readonly axios: AxiosInstance;
  constructor(
    @Inject(ABSTRACT_API) private readonly abstractApiKey: string,
    @Inject(EMAIL_VALIDATE_PUBLIC_API) private readonly publicAPI: string,
    private readonly logger: TracingLoggerService,
  ) {
    this.logger.setContext(EmailValidationHelper.name);
    this.axios = axios.create({
      baseURL: this.publicAPI,
      params: {
        api_key: this.abstractApiKey,
      },
    });
  }

  async validateEmail(email: string): Promise<boolean> {
    this.logger.debug('[CHECKING EMAIL] - Validate real email');
    const result = await this.axios.get('/', {
      params: {
        email: email,
      },
    });
    const data = result.data;
    if (data.error) {
      this.logger.error('Have error while checking email');
      throw new BadRequestException(data.error);
    }
    const allChecksPassed = [
      data.is_valid_format.value,
      data.is_free_email.value,
      !data.is_disposable_email.value,
      !data.is_role_email.value,
      !data.is_catchall_email.value,
      data.is_mx_found.value,
      data.is_smtp_valid.value,
    ].every((value) => value === true);

    this.logger.debug(
      `[CHECKING EMAIL] - Validate real email with result ${allChecksPassed}`,
    );
    return allChecksPassed;
  }
}