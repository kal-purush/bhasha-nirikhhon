import { TDataType } from '@weavcraft/common';
import { OpenAPI } from 'routing-controllers-openapi';

export const ApiResPaginatedSchema = <TModel extends TDataType<any>>(
  model: TModel,
  option?: {
    status?: number;
    description?: string;
    contentType?: string;
  },
) => {
  const properties = {
    type: 'array',
    items: { $ref: `#/components/schemas/${model.name}` },
  };
  const statuscode = option?.status ?? 200;
  const contentType = option?.contentType ?? 'application/json';

  return OpenAPI({
    description: option?.description,
    responses: {
      [`${statuscode}`]: {
        content: {
          [`${contentType}`]: {
            schema: {
              allOf: [
                {
                  $ref: `#/components/schemas/ApiResPaginatedDTO`,
                },
                {
                  properties: {
                    data: properties,
                  },
                },
              ],
            },
          },
        },
      },
    },
  });
};

export const ApiResDataSchema = <TModel extends TDataType<any>>(
  model: TModel,
  option?: {
    isArray?: boolean;
    status?: number;
    description?: string;
    contentType?: string;
  },
) => {
  const properties = option?.isArray
    ? {
        type: 'array',
        items: { $ref: `#/components/schemas/${model.name}` },
      }
    : {
        $ref: `#/components/schemas/${model.name}`,
      };
  const statuscode = option?.status ?? 200;
  const contentType = option?.contentType ?? 'application/json';

  return OpenAPI({
    description: option?.description,
    responses: {
      [`${statuscode}`]: {
        content: {
          [`${contentType}`]: {
            schema: {
              allOf: [
                {
                  $ref: `#/components/schemas/ApiResDataDTO`,
                },
                {
                  properties: {
                    data: properties,
                  },
                },
              ],
            },
          },
        },
      },
    },
  });
};

export const ApiResDataListSchema = <TModel extends TDataType<any>>(
  model: TModel,
  option?: {
    status?: number;
    description?: string;
    contentType?: string;
  },
) => {
  const properties = {
    type: 'array',
    items: { $ref: `#/components/schemas/${model.name}` },
  };
  const statuscode = option?.status ?? 200;
  const contentType = option?.contentType ?? 'application/json';

  return OpenAPI({
    description: option?.description,
    responses: {
      [`${statuscode}`]: {
        content: {
          [`${contentType}`]: {
            schema: {
              allOf: [
                {
                  $ref: `#/components/schemas/ApiResDataDTO`,
                },
                {
                  properties: {
                    data: properties,
                  },
                },
              ],
            },
          },
        },
      },
    },
  });
};