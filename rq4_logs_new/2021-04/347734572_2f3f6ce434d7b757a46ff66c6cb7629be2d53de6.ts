import S from 'fluent-json-schema';

import { STATUS } from '../constants';

export const responseSchema200 = S.object()
  .prop('status', S.const(STATUS.OK).required())
  .valueOf();

export const responseSchema4xx = S.object()
  .prop(
    'status',
    S.string()
      .enum([STATUS.OVERLAPPING, STATUS.INVALID, STATUS.ERROR])
      .required()
  )
  .valueOf();

const responseSchema500 = S.object()
  .prop('status', S.const(STATUS.ERROR).required())
  .valueOf();

export const defaultResponseSchema = {
  200: responseSchema200,
  '4xx': responseSchema4xx,
  500: responseSchema500,
};

export const rawUserSchema = S.object()
  .id('#user')
  .additionalProperties(false)
  .prop('id', S.string().required())
  .prop(
    'role',
    S.string()
      .enum([S.const('user'), S.const('admin')])
      .required()
  )
  .prop('firstName', S.string().required())
  .prop('lastName', S.string().required())
  .prop('email', S.string().format(S.FORMATS.EMAIL).required());

export const rawBaseEventSchema = S.object()
  .id('#baseEvent')
  .prop('start', S.string().format(S.FORMATS.DATE_TIME).required())
  .prop('end', S.string().format(S.FORMATS.DATE_TIME).required())
  .prop('description', S.string().required())
  .prop('allDay', S.boolean().required());

export const rawUserEventSchema = S.object()
  .additionalProperties(false)
  .prop('id', S.string().required())
  .prop('createdAt', S.string().format(S.FORMATS.DATE_TIME).required())
  .prop('resource', S.string().required())
  .extend(rawBaseEventSchema);

export const rawResourceSchema = S.object()
  .id('#resource')
  .prop('id', S.string().required())
  .prop('name', S.string().required());