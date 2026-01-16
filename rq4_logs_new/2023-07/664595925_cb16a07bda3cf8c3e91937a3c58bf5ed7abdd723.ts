import type { RosettyReturn } from 'rosetty';
import { defaultErrorMap, ZodErrorMap, ZodIssueCode, ZodParsedType } from 'zod';

export type ZodLocaleMap = {
  zod: {
    errors: {
      invalid_type: string;
      invalid_type_received_undefined: string;
      invalid_literal: string;
      unrecognized_keys: string;
      invalid_union: string;
      invalid_union_discriminator: string;
      invalid_enum_value: string;
      invalid_arguments: string;
      invalid_return_type: string;
      invalid_date: string;
      custom: string;
      invalid_intersection_types: string;
      not_multiple_of: string;
      not_finite: string;
      invalid_string: {
        email: string;
        url: string;
        uuid: string;
        cuid: string;
        regex: string;
        datetime: string;
        startsWith: string;
        endsWith: string;
      };
      too_small: {
        array: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
        string: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
        number: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
        set: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
        date: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
      };
      too_big: {
        array: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
        string: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
        number: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
        set: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
        date: {
          exact: string;
          inclusive: string;
          not_inclusive: string;
        };
      };
    };
    validations: {
      email: string;
      url: string;
      uuid: string;
      cuid: string;
      regex: string;
      datetime: string;
    };
    types: {
      function: string;
      number: string;
      string: string;
      nan: string;
      integer: string;
      float: string;
      boolean: string;
      date: string;
      bigint: string;
      undefined: string;
      symbol: string;
      null: string;
      array: string;
      object: string;
      unknown: string;
      promise: string;
      void: string;
      never: string;
      map: string;
      set: string;
    };
  };
};

const jsonStringifyReplacer = (_: string, value: unknown): unknown => {
  if (typeof value === 'bigint') {
    return value.toString();
  }
  return value;
};

function joinValues<T extends unknown[]>(array: T, separator = ' | '): string {
  return array
    .map((val) => (typeof val === 'string' ? `'${val}'` : val))
    .join(separator);
}

export const zodRosettyMap: ZodErrorMap = (issue, ctx) => {
  let message: string;
  message = defaultErrorMap(issue, ctx).message;

  switch (issue.code) {
    case ZodIssueCode.invalid_type:
      if (issue.received === ZodParsedType.undefined) {
        message = `zod.errors.invalid_type_received_undefined`;
      } else {
        message = `zod.errors.invalid_type - ${JSON.stringify({
          expected: `zod.types.${issue.expected}`,
          received: `zod.types.${issue.received}`,
        })}`;
      }
      break;
    case ZodIssueCode.invalid_literal:
      message = `zod.errors.invalid_literal - ${JSON.stringify({
        expected: JSON.stringify(issue.expected, jsonStringifyReplacer),
      })}`;
      break;
    case ZodIssueCode.unrecognized_keys:
      message = `zod.errors.unrecognized_keys - ${JSON.stringify({
        keys: joinValues(issue.keys, ', '),
        count: issue.keys.length,
      })}`;
      break;
    case ZodIssueCode.invalid_union:
      message = 'zod.errors.invalid_union';
      break;
    case ZodIssueCode.invalid_union_discriminator:
      message = `zod.errors.invalid_union_discriminator - ${JSON.stringify({
        options: joinValues(issue.options),
      })}`;
      break;
    case ZodIssueCode.invalid_enum_value:
      message = `zod.errors.invalid_enum_value - ${JSON.stringify({
        options: joinValues(issue.options),
        received: issue.received,
      })}`;
      break;
    case ZodIssueCode.invalid_arguments:
      message = 'zod.errors.invalid_arguments';
      break;
    case ZodIssueCode.invalid_return_type:
      message = 'zod.errors.invalid_return_type';
      break;
    case ZodIssueCode.invalid_date:
      message = 'zod.errors.invalid_date';
      break;
    case ZodIssueCode.invalid_string:
      if (typeof issue.validation === 'object') {
        if ('startsWith' in issue.validation) {
          message = `zod.errors.invalid_string.startsWith - ${JSON.stringify({
            startsWith: issue.validation.startsWith,
          })}`;
        } else if ('endsWith' in issue.validation) {
          message = `zod.errors.invalid_string.endsWith - ${JSON.stringify({
            endsWith: issue.validation.endsWith,
          })}`;
        }
      } else {
        message = `zod.errors.invalid_string.${
          issue.validation
        } - ${JSON.stringify({
          validation: `zod.validations.${issue.validation}`,
        })}`;
      }
      break;
    case ZodIssueCode.too_small:
      // eslint-disable-next-line no-case-declarations
      const minimum =
        issue.type === 'date'
          ? new Date(issue.minimum as number)
          : issue.minimum;
      message = `zod.errors.too_small.${issue.type}.${
        issue.exact ? 'exact' : issue.inclusive ? 'inclusive' : 'not_inclusive'
      } - ${JSON.stringify({
        minimum,
        count: typeof minimum === 'number' ? minimum : undefined,
      })}`;
      break;
    case ZodIssueCode.too_big:
      // eslint-disable-next-line no-case-declarations
      const maximum =
        issue.type === 'date'
          ? new Date(issue.maximum as number)
          : issue.maximum;
      message = `zod.errors.too_big.${issue.type}.${
        issue.exact ? 'exact' : issue.inclusive ? 'inclusive' : 'not_inclusive'
      } - ${JSON.stringify({
        maximum,
        count: typeof maximum === 'number' ? maximum : undefined,
      })}`;
      break;
    case ZodIssueCode.custom:
      message = 'errors.custom';
      break;
    case ZodIssueCode.invalid_intersection_types:
      message = 'zod.errors.invalid_intersection_types';
      break;
    case ZodIssueCode.not_multiple_of:
      message = `zod.errors.not_multiple_of - ${JSON.stringify({
        multipleOf: issue.multipleOf,
      })}`;
      break;
    case ZodIssueCode.not_finite:
      message = 'zod.errors.not_finite';
      break;
    default:
  }

  return { message };
};

export const translateZodErrorMessage = (
  error: { message: string | ZodErrorMap } | undefined,
  t: RosettyReturn<ZodLocaleMap>['t'],
) => {
  if (!error || !t) {
    return undefined;
  }

  const message =
    typeof error.message === 'string' && !error.message.startsWith('[')
      ? error.message
      : //@ts-ignore
        JSON.parse(error.message)[0].message;

  const [key, valuesString] = message.split('-').map((i: string) => i.trim());

  if (!valuesString) {
    //@ts-ignore
    return t(key);
  }
  const values = JSON.parse(valuesString);

  for (const [k, v] of Object.entries(values)) {
    if (typeof v === 'string' && v.startsWith('zod.')) {
      const [key, valuesString] = v.split('-').map((i) => i.trim());
      //@ts-ignore
      values[k] = t(key, valuesString ? JSON.parse(valuesString) : {});
      continue;
    }
  }

  //@ts-ignore
  return t(key, values);
};