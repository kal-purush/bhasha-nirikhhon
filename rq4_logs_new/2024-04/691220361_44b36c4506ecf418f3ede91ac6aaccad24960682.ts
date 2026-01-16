import { z } from 'zod';

export const autoImportFrequencyEnum = ['daily', 'weekly', 'monthly'] as const;

export type AutoImportFrequencyType = (typeof autoImportFrequencyEnum)[number];

export const autoImportFrequencyEnumSelection = [
	{ value: 'daily', name: 'Daily' },
	{ value: 'weekly', name: 'Weekly' },
	{ value: 'monthly', name: 'Monthly' }
] satisfies { value: AutoImportFrequencyType; name: string }[];

export const autoImportFrequencyToDisplay = (status: AutoImportFrequencyType) => {
	return (
		autoImportFrequencyEnumSelection.find((currentStatus) => currentStatus.value === status)
			?.name ?? status
	);
};

export const autoImportTypes = ['saltedge', 'akahu'] as const;

export type AutoImportTypesType = (typeof autoImportTypes)[number];

export const autoImportTypeDropdown = [
	{ value: 'saltedge', name: 'Salt Edge' },
	{ value: 'akahu', name: 'Akahu' }
] satisfies { value: AutoImportTypesType; name: string }[];

export const autoImportTypeToDisplay = (type: AutoImportTypesType) => {
	return autoImportTypeDropdown.find((currentStatus) => currentStatus.value === type)?.name ?? type;
};

export const autoImportFormItemDisplay: {
	[K in AutoImportTypesType]: {
		connectionId: boolean;
		accountId: boolean;
		startDate: boolean;
		appId: boolean;
		secret: boolean;
		lookbackDays: boolean;
		userAccessToken: boolean;
		appAccessToken: boolean;
	};
} = {
	akahu: {
		userAccessToken: true,
		appAccessToken: true,
		accountId: true,
		startDate: true,
		lookbackDays: true,
		connectionId: false,
		appId: false,
		secret: false
	},
	saltedge: {
		connectionId: true,
		accountId: true,
		startDate: true,
		appId: true,
		secret: true,
		lookbackDays: true,
		userAccessToken: false,
		appAccessToken: false
	}
};

export const autoImportSaltEdgeSchema = z.object({
	type: z.literal('saltedge'),
	connectionId: z.string(),
	accountId: z.string(),
	startDate: z
		.string()
		.regex(/^\d{4}-\d{2}-\d{2}$/)
		.optional(),
	appId: z.string(),
	secret: z.string(),
	lookbackDays: z.number().optional().default(5)
});

export type AutoImportSaltEdgeSchemaType = z.infer<typeof autoImportSaltEdgeSchema>;

export const autoImportAkahu = z.object({
	type: z.literal('akahu'),
	userAccessToken: z.string(),
	appAccessToken: z.string(),
	accountId: z.string(),
	startDate: z
		.string()
		.regex(/^\d{4}-\d{2}-\d{2}$/)
		.optional(),
	lookbackDays: z.number().optional().default(5)
});

export type AutoImportAkahuSchemaType = z.infer<typeof autoImportAkahu>;

export const autoImportCombinedSchema = z.union([autoImportSaltEdgeSchema, autoImportAkahu]);

export type AutoImportCombinedSchemaType = z.infer<typeof autoImportCombinedSchema>;

export const createAutoImportSchema = z.object({
	title: z.string(),
	enabled: z.boolean().optional().default(false),
	importMappingId: z.string(),
	frequency: z.enum(autoImportFrequencyEnum).optional().default('daily'),
	type: z.enum(autoImportTypes),
	config: autoImportCombinedSchema,
	autoProcess: z.boolean().optional().default(true),
	autoClean: z.boolean().optional().default(true)
});

export const createAutoImportFormSchema = z.object({
	title: z.string(),
	enabled: z.boolean().optional().default(false),
	importMappingId: z.string(),
	frequency: z.enum(autoImportFrequencyEnum).optional().default('daily'),
	type: z.enum(autoImportTypes),
	connectionId: z.string().optional(),
	accountId: z.string().optional(),
	startDate: z
		.string()
		.regex(/^\d{4}-\d{2}-\d{2}$/)
		.optional(),
	appId: z.string().optional(),
	secret: z.string().optional(),
	lookbackDays: z.number().optional().default(5),
	userAccessToken: z.string().optional(),
	appAccessToken: z.string().optional(),
	autoProcess: z.boolean().optional(),
	autoClean: z.boolean().optional()
});

export type CreateAutoImportSchemaType = z.infer<typeof createAutoImportSchema>;
export type CreateAutoImportFormSchemaType = z.infer<typeof createAutoImportFormSchema>;

export const updateAutoImportSchema = z.object({
	title: z.string().optional(),
	enabled: z.boolean().optional(),
	importMappingId: z.string().optional(),
	frequency: z.enum(autoImportFrequencyEnum).optional(),
	config: autoImportCombinedSchema.optional(),
	autoProcess: z.boolean().optional(),
	autoClean: z.boolean().optional()
});

export const updateAutoImportFormSchema = createAutoImportFormSchema
	.partial()
	.merge(z.object({ id: z.string() }));

export type UpdateAutoImportFormSchemaType = z.infer<typeof updateAutoImportFormSchema>;

export type UpdateAutoImportSchemaType = z.infer<typeof updateAutoImportSchema>;

export const autoImportOrderByOptions = [
	'createdAt',
	'updatedAt',
	'title',
	'autoProcess',
	'autoClean',
	'type',
	'frequency',
	'importMapping',
	'enabled'
] as const;

export type AutoImportOrderByOptionsType = (typeof autoImportOrderByOptions)[number];

export const autoImportFilterSchema = z.object({
	id: z.string().optional(),
	title: z.string().optional(),
	autoProcess: z.boolean().optional(),
	autoClean: z.boolean().optional(),
	enabled: z.boolean().optional(),
	importMappingId: z.string().optional(),
	frequency: z.array(z.enum(autoImportFrequencyEnum)).optional(),
	type: z.array(z.enum(autoImportTypes)).optional(),
	page: z.number().default(0).optional(),
	pageSize: z.number().default(10).optional(),
	orderBy: z
		.array(
			z.object({
				field: z.enum(autoImportOrderByOptions),
				direction: z.enum(['asc', 'desc'])
			})
		)
		.default([{ direction: 'desc', field: 'createdAt' }])
		.optional()
});

export type AutoImportFilterSchemaType = z.infer<typeof autoImportFilterSchema>;