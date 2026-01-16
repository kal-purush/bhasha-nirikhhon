import { z } from 'zod';

import { FILTER_KIND } from './constants';

export const filterConfigSharedPropsSchema = z.object({
  position: z.number(),
  label: z.string(),
  show: z.boolean(),
  googleAnalyticsEventName: z.string().nullable(),
});
export type FilterConfigSharedProps = z.infer<
  typeof filterConfigSharedPropsSchema
>;

const rangeValueSchema = z.object({
  value: z.number().min(0),
  paramKey: z.string(),
});

export const rangeFilterConfigSchema = z.intersection(
  filterConfigSharedPropsSchema,
  z.object({
    kind: z.literal(FILTER_KIND.RANGE),
    value: z.object({
      lowest: rangeValueSchema,
      highest: rangeValueSchema,
    }),
    prefix: z.string().nullable(),
  }),
);
export type RangeFilterConfig = z.infer<typeof rangeFilterConfigSchema>;

const selectOptionsSchema = z.array(
  z.object({
    label: z.string(),
    value: z.union([z.string(), z.boolean()]),
  }),
);
export type SelectOptions = z.infer<typeof selectOptionsSchema>;

export const singleSelectFilterConfigSchema = z.intersection(
  filterConfigSharedPropsSchema,
  z.object({
    kind: z.literal(FILTER_KIND.SINGLE_SELECT),
    paramKey: z.string(),
    options: selectOptionsSchema,
  }),
);
export type SingleSelectFilterConfig = z.infer<
  typeof singleSelectFilterConfigSchema
>;

export const multiSelectFilterConfigSchema = z.intersection(
  filterConfigSharedPropsSchema,
  z.object({
    kind: z.union([
      z.literal(FILTER_KIND.MULTI_SELECT),
      z.literal(FILTER_KIND.MULTI_SELECT_WITH_SEARCH),
    ]),
    paramKey: z.string(),
    options: z.array(z.string()),
  }),
);
export type MultiSelectFilterConfig = z.infer<
  typeof multiSelectFilterConfigSchema
>;

export const filterConfigSchema = z.union([
  rangeFilterConfigSchema,
  singleSelectFilterConfigSchema,
  multiSelectFilterConfigSchema,
]);
export type FilterConfig = z.infer<typeof filterConfigSchema>;

export const filterConfigResponseSchema = z.record(
  z.union([
    rangeFilterConfigSchema,
    singleSelectFilterConfigSchema,
    multiSelectFilterConfigSchema,
  ]),
);
export type FilterConfigResponse = z.infer<typeof filterConfigResponseSchema>;