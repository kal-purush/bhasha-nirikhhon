import { plainToClass } from 'class-transformer';
import { IsUrl, validateSync } from 'class-validator';

export class EnvironmentVariables {
  @IsUrl()
  RPC_URL: string;
}

export function validate(config: Record<string, unknown>) {
  const validatedConfig = plainToClass(EnvironmentVariables, config);

  const validatorOptions = { skipMissingProperties: false };
  const errors = validateSync(validatedConfig, validatorOptions);

  if (errors.length > 0) {
    console.error(errors.toString());
    process.exit(1);
  }

  return validatedConfig;
}