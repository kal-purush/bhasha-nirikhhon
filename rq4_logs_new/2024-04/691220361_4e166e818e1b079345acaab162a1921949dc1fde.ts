import { z } from 'zod';

const importDetailStatusEnum = [
	'error',
	'processed',
	'duplicate',
	'imported',
	'importError'
] as const;

const importStatusEnum = [
	'created',
	'error',
	'processed',
	'importing',
	'awaitingImport',
	'complete'
] as const;

const importTypeEnum = [
	'transaction',
	'account',
	'bill',
	'budget',
	'category',
	'tag',
	'label',
	'mappedImport'
] as const;

const importSourceEnum = ['csv', 'json'] as const;

const accountTypeEnum = ['income', 'expense', 'asset', 'liability'] as const;

const reusableFilterModifcationType = ['replace', 'modify'] as const;
const statusEnum = ['active', 'disabled'] as const;

const currencyFormatEnum = ['USD', 'GBP', 'INR', 'AUD', 'EUR'] as const;

const dateFormatEnum = ['YYYY-MM-DD', 'MM/DD/YY', 'MM/DD/YYYY', 'DD/MM/YY', 'DD/MM/YYYY'] as const;

const idColumn = { id: z.string() };

const dateSpanEnum = [
	'allTime',
	'thisWeek',
	'lastWeek',
	'thisMonth',
	'lastMonth',
	'thisQuarter',
	'lastQuarter',
	'thisYear',
	'lastYear',
	'thisFinancialYear',
	'lastFinancialYear',
	'fromOneMonthAgo',
	'fromThreeMonthsAgo',
	'fromSixMonthsAgo',
	'fromOneYearAgo',
	'fromTwoYearsAgo'
] as const;

const dateStringSchema = z.coerce
	.string()
	.trim()
	.length(10)
	.regex(new RegExp(/^\d{4}-\d{2}-\d{2}/));

const summaryFilterProperties = {
	countMax: z.number().optional(),
	countMin: z.number().optional(),
	totalMax: z.number().optional(),
	totalMin: z.number().optional(),
	firstDateMin: dateStringSchema.optional().nullable(),
	firstDateMax: dateStringSchema.optional().nullable(),
	lastDateMin: dateStringSchema.optional().nullable(),
	lastDateMax: dateStringSchema.optional().nullable()
};

const accountFilterSchema = z.object({
	textFilter: z.string().optional(),
	id: z.string().optional(),
	idArray: z.array(z.string()).optional(),
	excludeIdArray: z.array(z.string()).optional(),
	title: z.coerce.string().optional(),
	titleArray: z.array(z.coerce.string()).optional(),
	excludeTitleArray: z.array(z.coerce.string()).optional(),
	type: z
		.array(z.enum(accountTypeEnum))
		.optional()
		.transform((e) => (e && e.length === 0 ? undefined : e)),
	excludeType: z
		.array(z.enum(accountTypeEnum))
		.optional()
		.transform((e) => (e && e.length === 0 ? undefined : e)),
	accountGroup: z.coerce.string().optional(),
	accountGroupArray: z.array(z.coerce.string()).optional(),
	excludeAccountGroupArray: z.array(z.coerce.string()).optional(),
	accountGroup2: z.coerce.string().optional(),
	accountGroup2Array: z.array(z.coerce.string()).optional(),
	excludeAccountGroup2Array: z.array(z.coerce.string()).optional(),
	accountGroup3: z.coerce.string().optional(),
	accountGroup3Array: z.array(z.coerce.string()).optional(),
	excludeAccountGroup3Array: z.array(z.coerce.string()).optional(),
	accountGroupCombined: z.coerce.string().optional(),
	accountGroupCombinedArray: z.array(z.string()).optional(),
	excludeAccountGroupCombinedArray: z.array(z.string()).optional(),
	accountTitleCombined: z.coerce.string().optional(),
	accountTitleCombinedArray: z.array(z.string()).optional(),
	excludeAccountTitleCombinedArray: z.array(z.string()).optional(),
	status: z.enum(statusEnum).optional(),
	statusArray: z.array(z.enum(statusEnum)).optional(),
	excludeStatusArray: z.array(z.enum(statusEnum)).optional(),
	disabled: z.boolean().optional(),
	allowUpdate: z.boolean().optional(),
	active: z.boolean().optional(),
	isCash: z.coerce.boolean().optional(),
	isNetWorth: z.coerce.boolean().optional(),
	startDateBefore: dateStringSchema.optional(),
	startDateAfter: dateStringSchema.optional(),
	endDateBefore: dateStringSchema.optional(),
	endDateAfter: dateStringSchema.optional(),
	importIdArray: z.array(z.string()).optional(),
	excludeImportIdArray: z.array(z.string()).optional(),
	importDetailIdArray: z.array(z.string()).optional(),
	excludeImportDetailIdArray: z.array(z.string()).optional(),

	//Summary Info Filters
	...summaryFilterProperties
});

const tagFilterSchema = z.object({
	textFilter: z.string().optional(),
	id: z.string().optional(),
	idArray: z.array(z.string()).optional(),
	excludeIdArray: z.array(z.string()).optional(),
	title: z.coerce.string().optional(),
	titleArray: z.array(z.coerce.string()).optional(),
	excludeTitleArray: z.array(z.coerce.string()).optional(),
	group: z.coerce.string().optional(),
	groupArray: z.array(z.coerce.string()).optional(),
	excludeGroupArray: z.array(z.coerce.string()).optional(),
	single: z.coerce.string().optional(),
	singleArray: z.array(z.coerce.string()).optional(),
	excludeSingleArray: z.array(z.coerce.string()).optional(),
	status: z.enum(statusEnum).optional(),
	statusArray: z.array(z.enum(statusEnum)).optional(),
	excludeStatusArray: z.array(z.enum(statusEnum)).optional(),
	disabled: z.boolean().optional(),
	allowUpdate: z.boolean().optional(),
	active: z.boolean().optional(),
	importIdArray: z.array(z.string()).optional(),
	excludeImportIdArray: z.array(z.string()).optional(),
	importDetailIdArray: z.array(z.string()).optional(),
	excludeImportDetailIdArray: z.array(z.string()).optional(),

	//Summary Info Filters
	...summaryFilterProperties
});

const billFilterSchema = z.object({
	textFilter: z.string().optional(),
	id: z.string().optional(),
	idArray: z.array(z.string()).optional(),
	excludeIdArray: z.array(z.string()).optional(),
	title: z.coerce.string().optional(),
	titleArray: z.array(z.coerce.string()).optional(),
	excludeTitleArray: z.array(z.coerce.string()).optional(),
	status: z.enum(statusEnum).optional(),
	statusArray: z.array(z.enum(statusEnum)).optional(),
	excludeStatusArray: z.array(z.enum(statusEnum)).optional(),
	disabled: z.boolean().optional(),
	allowUpdate: z.boolean().optional(),
	active: z.boolean().optional(),
	importIdArray: z.array(z.string()).optional(),
	excludeImportIdArray: z.array(z.string()).optional(),
	importDetailIdArray: z.array(z.string()).optional(),
	excludeImportDetailIdArray: z.array(z.string()).optional(),

	//Summary Info Filters
	...summaryFilterProperties
});

const budgetFilterSchema = z.object({
	textFilter: z.string().optional(),
	id: z.string().optional(),
	idArray: z.array(z.string()).optional(),
	excludeIdArray: z.array(z.string()).optional(),
	title: z.coerce.string().optional(),
	titleArray: z.array(z.coerce.string()).optional(),
	excludeTitleArray: z.array(z.coerce.string()).optional(),
	status: z.enum(statusEnum).optional(),
	statusArray: z.array(z.enum(statusEnum)).optional(),
	excludeStatusArray: z.array(z.enum(statusEnum)).optional(),
	disabled: z.boolean().optional(),
	allowUpdate: z.boolean().optional(),
	active: z.boolean().optional(),
	importIdArray: z.array(z.string()).optional(),
	excludeImportIdArray: z.array(z.string()).optional(),
	importDetailIdArray: z.array(z.string()).optional(),
	excludeImportDetailIdArray: z.array(z.string()).optional(),

	//Summary Info Filters
	...summaryFilterProperties
});

const categoryFilterSchema = z.object({
	textFilter: z.string().optional(),
	id: z.string().optional(),
	idArray: z.array(z.string()).optional(),
	excludeIdArray: z.array(z.string()).optional(),
	title: z.coerce.string().optional(),
	titleArray: z.array(z.coerce.string()).optional(),
	excludeTitleArray: z.array(z.coerce.string()).optional(),
	group: z.coerce.string().optional(),
	groupArray: z.array(z.coerce.string()).optional(),
	excludeGroupArray: z.array(z.coerce.string()).optional(),
	single: z.coerce.string().optional(),
	singleArray: z.array(z.coerce.string()).optional(),
	excludeSingleArray: z.array(z.coerce.string()).optional(),
	status: z.enum(statusEnum).optional(),
	statusArray: z.array(z.enum(statusEnum)).optional(),
	excludeStatusArray: z.array(z.enum(statusEnum)).optional(),
	disabled: z.boolean().optional(),
	allowUpdate: z.boolean().optional(),
	active: z.boolean().optional(),
	importIdArray: z.array(z.string()).optional(),
	excludeImportIdArray: z.array(z.string()).optional(),
	importDetailIdArray: z.array(z.string()).optional(),
	excludeImportDetailIdArray: z.array(z.string()).optional(),

	//Summary Info Filters
	...summaryFilterProperties
});

const labelFilterSchema = z.object({
	textFilter: z.string().optional(),
	id: z.string().optional(),
	idArray: z.array(z.string()).optional(),
	excludeIdArray: z.array(z.string()).optional(),
	title: z.coerce.string().optional(),
	titleArray: z.array(z.string()).optional(),
	excludeTitleArray: z.array(z.string()).optional(),
	status: z.enum(statusEnum).optional(),
	statusArray: z.array(z.enum(statusEnum)).optional(),
	excludeStatusArray: z.array(z.enum(statusEnum)).optional(),
	disabled: z.boolean().optional(),
	allowUpdate: z.boolean().optional(),
	active: z.boolean().optional(),
	importIdArray: z.array(z.string()).optional(),
	excludeImportIdArray: z.array(z.string()).optional(),
	importDetailIdArray: z.array(z.string()).optional(),
	excludeImportDetailIdArray: z.array(z.string()).optional(),

	//Summary Info Filters
	...summaryFilterProperties
});

const journalFilterSchemaWithoutPagination = z.object({
	textFilter: z.coerce.string().optional(),
	id: z.coerce.string().optional(),
	excludeId: z.coerce.string().optional(),
	idArray: z.array(z.string()).optional(),
	excludeIdArray: z.array(z.string()).optional(),
	transactionIdArray: z.array(z.string()).optional(),
	excludeTransactionIdArray: z.array(z.string()).optional(),
	dateSpan: z.enum(dateSpanEnum).optional().nullable(),
	dateBefore: dateStringSchema.optional().nullable(),
	dateAfter: dateStringSchema.optional().nullable(),
	maxAmount: z.number().optional(),
	minAmount: z.number().optional(),
	yearMonth: z.array(z.string()).optional(),
	excludeYearMonth: z.array(z.string()).optional(),
	description: z.coerce.string().optional().nullable(),
	descriptionArray: z.array(z.string()).optional(),
	excludeDescription: z.coerce.string().optional(),
	excludeDescriptionArray: z.array(z.string()).optional(),
	transfer: z.coerce.boolean().optional(),
	linked: z.coerce.boolean().optional(),
	reconciled: z.coerce.boolean().optional(),
	dataChecked: z.coerce.boolean().optional(),
	complete: z.boolean().optional(),
	importIdArray: z.array(z.string()).optional(),
	importDetailIdArray: z.array(z.string()).optional(),
	payee: z
		.object({
			id: z.string().optional(),
			idArray: z.array(z.string()).optional(),
			title: z.string().optional(),
			titleArray: z.array(z.string()).optional()
		})
		.optional(),
	excludePayee: z
		.object({
			id: z.string().optional(),
			idArray: z.array(z.string()).optional(),
			title: z.string().optional(),
			titleArray: z.array(z.string()).optional()
		})
		.optional(),
	account: accountFilterSchema
		.optional()
		.default({ type: ['asset', 'liability'] })
		.optional(),
	excludeAccount: accountFilterSchema.optional(),
	tag: tagFilterSchema.optional(),
	excludeTag: tagFilterSchema.optional(),
	bill: billFilterSchema.optional(),
	excludeBill: billFilterSchema.optional(),
	budget: budgetFilterSchema.optional(),
	excludeBudget: budgetFilterSchema.optional(),
	category: categoryFilterSchema.optional(),
	excludeCategory: categoryFilterSchema.optional(),
	label: labelFilterSchema.optional(),
	excludeLabel: labelFilterSchema.optional()
});

const reportElementLayoutEnum = [
	'singleItem',
	'twoItemsHorizontal',
	'twoItemsVertical',
	'twoSmallOneLargeVertical',
	'twoSmallOneLargeHorizontal'
] as const;

const statusColumns = {
	status: z.enum(statusEnum),
	active: z.boolean(),
	disabled: z.boolean(),
	allowUpdate: z.boolean()
};

const importColumns = {
	importId: z.string().nullable(),
	importDetailId: z.string().nullable()
};

const timestampColumns = {
	createdAt: z.date(),
	updatedAt: z.date()
};

const reportConfigPartTimeGroupingEnum = ['year', 'month', 'quarter', 'week'] as const;

const reportConfigPartItemGroupingEnum = [
	'account',
	'account_type',
	'account_group',
	'account_group_2',
	'account_group_3',
	'account_group_combined',
	'account_title',
	'tag',
	'tag_group',
	'tag_single',
	'category',
	'category_group',
	'category_single',
	'bill',
	'budget',
	'none'
] as const;

const reportConfigPartNumberDisplayEnum = [
	'number',
	'currency',
	'percent',
	'number2dp',
	'percent2dp'
] as const;

const reportConfigPartTrendDisplayEnum = ['all', 'top10', 'nonBlank', 'top10nonBlank'] as const;

const reportConfigPartNegativeDisplayEnum = ['hide', 'grouped', 'absolute', 'default'] as const;

const reportConfigPartSchema_None = z.object({
	id: z.string(),
	order: z.number(),
	type: z.literal('none')
});

const reportConfigPartSchema_NumberCurrency = z.object({
	id: z.string(),
	order: z.number(),
	type: z.literal('number'),
	mathConfig: z.string(),
	numberDisplay: z.enum(reportConfigPartNumberDisplayEnum).default('number')
});

const reportConfigPartSchema_String = z.object({
	id: z.string(),
	order: z.number(),
	type: z.literal('string'),
	stringConfig: z.string(),
	numberDisplay: z.enum(reportConfigPartNumberDisplayEnum).default('number')
});

const reportConfigPartSchema_Sparkline = z.object({
	id: z.string(),
	order: z.number(),
	type: z.literal('sparkline').or(z.literal('sparklinebar')),
	mathConfig: z.string(),
	timeGrouping: z.enum(reportConfigPartTimeGroupingEnum).default('month'),
	numberDisplay: z.enum(reportConfigPartNumberDisplayEnum).default('number')
});

const reportConfigPartSchema_TimeGraph = z.object({
	id: z.string(),
	order: z.number(),
	type: z.literal('time_line').or(z.literal('time_stackedArea')).or(z.literal('time_bar')),
	mathConfig: z.string(),
	timeGrouping: z.enum(reportConfigPartTimeGroupingEnum).default('month'),
	itemGrouping: z.enum(reportConfigPartItemGroupingEnum).optional(),
	numberDisplay: z.enum(reportConfigPartNumberDisplayEnum).default('number'),
	trendDisplay: z.enum(reportConfigPartTrendDisplayEnum).default('all'),
	includeTotal: z.boolean().default(false)
});

const reportConfigPartSchema_NonTimeGraph = z.object({
	id: z.string(),
	order: z.number(),
	type: z.literal('pie').or(z.literal('box')),
	mathConfig: z.string(),
	itemGrouping: z.enum(reportConfigPartItemGroupingEnum),
	itemGrouping2: z.enum(reportConfigPartItemGroupingEnum).default('none'),
	itemGrouping3: z.enum(reportConfigPartItemGroupingEnum).default('none'),
	itemGrouping4: z.enum(reportConfigPartItemGroupingEnum).default('none'),
	numberDisplay: z.enum(reportConfigPartNumberDisplayEnum).default('number'),
	negativeDisplay: z.enum(reportConfigPartNegativeDisplayEnum).default('default'),
	trendDisplay: z.enum(reportConfigPartTrendDisplayEnum).default('all')
});

const reportConfigPartIndividualSchema = z.union([
	reportConfigPartSchema_None,
	reportConfigPartSchema_NumberCurrency,
	reportConfigPartSchema_String,
	reportConfigPartSchema_Sparkline,
	reportConfigPartSchema_TimeGraph,
	reportConfigPartSchema_NonTimeGraph
]);

const reportConfigPartSchema = z.array(reportConfigPartIndividualSchema);

export const autoImportFrequencyEnum = ['daily', 'weekly', 'monthly'] as const;
export const autoImportTypes = ['saltedge', 'akahu'] as const;

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

export const autoImportCombinedSchema = z.union([autoImportSaltEdgeSchema, autoImportAkahu]);

const pageSizeEnum = ['sm', 'lg', 'xs', 'xl'] as const;

export const backupSchemaRev09 = z.object({
	version: z.literal(9),
	information: z.object({
		createdAt: z.date(),
		title: z.string(),
		creationReason: z.string(),
		createdBy: z.string(),
		itemCount: z.object({
			numberAccounts: z.number(),
			numberBills: z.number(),
			numberBudgets: z.number(),
			numberCategories: z.number(),
			numberTransactions: z.number(),
			numberJournalEntries: z.number(),
			numberLabels: z.number(),
			numberLabelsToJournals: z.number(),
			numberTags: z.number(),
			numberImportItemDetails: z.number(),
			numberImportTables: z.number(),
			numberImportMappings: z.number(),
			numberReusableFilters: z.number(),
			numberReports: z.number(),
			numberReportFilters: z.number(),
			numberReportElements: z.number(),
			numberReportItems: z.number(),
			numberKeyValues: z.number(),
			numberBackups: z.number(),
			numberAutoImport: z.number()
		})
	}),
	data: z.object({
		user: z.array(
			z.object({
				id: z.string(),
				name: z.string(),
				username: z.string(),
				admin: z.boolean(),
				currencyFormat: z.enum(currencyFormatEnum),
				dateFormat: z.enum(dateFormatEnum)
			})
		),
		session: z.array(
			z.object({
				id: z.string(),
				userId: z.string(),
				expiresAt: z.date()
			})
		),
		key: z.array(
			z.object({
				id: z.string(),
				userId: z.string(),
				hashedPassword: z.string().nullable()
			})
		),
		account: z.array(
			z.object({
				...idColumn,
				...importColumns,
				title: z.string(),
				type: z.enum(accountTypeEnum),
				isCash: z.boolean(),
				isNetWorth: z.boolean(),
				accountGroup: z.string(),
				accountGroup2: z.string(),
				accountGroup3: z.string(),
				accountGroupCombined: z.string(),
				accountTitleCombined: z.string(),
				startDate: z.string().nullable(),
				endDate: z.string().nullable(),
				...statusColumns,
				...timestampColumns
			})
		),
		tag: z.array(
			z.object({
				...idColumn,
				...importColumns,
				title: z.string(),
				group: z.string(),
				single: z.string(),
				...statusColumns,
				...timestampColumns
			})
		),
		category: z.array(
			z.object({
				...idColumn,
				...importColumns,
				title: z.string(),
				group: z.string(),
				single: z.string(),
				...statusColumns,
				...timestampColumns
			})
		),
		bill: z.array(
			z.object({
				...idColumn,
				...importColumns,
				title: z.string(),
				...statusColumns,
				...timestampColumns
			})
		),
		budget: z.array(
			z.object({
				...idColumn,
				...importColumns,
				title: z.string(),
				...statusColumns,
				...timestampColumns
			})
		),
		label: z.array(
			z.object({
				...idColumn,
				...importColumns,
				title: z.string(),
				...statusColumns,
				...timestampColumns
			})
		),
		labelsToJournals: z.array(
			z.object({
				...idColumn,
				labelId: z.string(),
				journalId: z.string(),
				...timestampColumns
			})
		),
		transaction: z.array(
			z.object({
				...idColumn,
				...timestampColumns
			})
		),
		journalEntry: z.array(
			z.object({
				...idColumn,
				...importColumns,
				uniqueId: z.string().nullable(),
				amount: z.number(),
				transactionId: z.string(),
				description: z.string(),
				date: z.date(),
				dateText: z.string(),
				tagId: z.string().nullable(),
				billId: z.string().nullable(),
				budgetId: z.string().nullable(),
				categoryId: z.string().nullable(),
				accountId: z.string(),

				yearMonthDay: z.string(),
				yearWeek: z.string(),
				yearMonth: z.string(),
				yearQuarter: z.string(),
				year: z.string(),

				linked: z.boolean(),
				reconciled: z.boolean(),
				dataChecked: z.boolean(),
				complete: z.boolean(),

				transfer: z.boolean(),

				...timestampColumns
			})
		),
		importItemDetail: z.array(
			z.object({
				...idColumn,
				importId: z.string(),
				status: z.enum(importDetailStatusEnum),
				duplicateId: z.string().nullable(),
				uniqueId: z.string().nullable(),
				relationId: z.string().nullable(),
				relation2Id: z.string().nullable(),
				importInfo: z.unknown(),
				processedInfo: z
					.object({
						dataToUse: z.record(z.unknown()).optional(),
						source: z.record(z.unknown()).optional(),
						processed: z.record(z.unknown()).optional()
					})
					.nullable()
					.optional(),
				errorInfo: z
					.object({
						error: z.record(z.unknown()).optional(),
						errors: z.array(z.string()).optional()
					})
					.nullable()
					.optional(),
				...timestampColumns
			})
		),
		importTable: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				title: z.string(),
				filename: z.string().nullable(),
				checkImportedOnly: z.boolean(),
				status: z.enum(importStatusEnum),
				statusText: z.string().optional(),
				source: z.enum(importSourceEnum),
				type: z.enum(importTypeEnum),
				importMappingId: z.string().nullable(),
				errorInfo: z.unknown(),
				autoProcess: z.boolean(),
				autoClean: z.boolean(),
				importState: z
					.object({
						startTime: z.date(),
						count: z.number(),
						ids: z.array(z.string()),
						complete: z.number(),
						completeIds: z.array(z.string())
					})
					.nullable()
					.optional()
			})
		),
		autoImportTable: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				title: z.string(),
				enabled: z.boolean(),
				importMappingId: z.string(),
				frequency: z.enum(autoImportFrequencyEnum),
				type: z.enum(autoImportTypes),
				lastTransactionDate: z.date().nullable(),
				autoProcess: z.boolean(),
				autoClean: z.boolean(),
				config: autoImportCombinedSchema
			})
		),
		reusableFilter: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				title: z.string(),
				group: z.string().nullable(),
				journalCount: z.number(),
				canApply: z.boolean(),
				needsUpdate: z.boolean(),
				applyAutomatically: z.boolean(),
				applyFollowingImport: z.boolean(),
				listed: z.boolean(),
				modificationType: z.enum(reusableFilterModifcationType).nullable(),
				filter: z.string(),
				filterText: z.string(),
				change: z.string().nullable(),
				changeText: z.string().nullable()
			})
		),
		importMapping: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				title: z.string(),
				configuration: z.string(),
				sampleData: z.string().nullable()
			})
		),
		filter: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				filterText: z.string(),
				filter: journalFilterSchemaWithoutPagination
			})
		),

		reportElementConfig: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				title: z.string().nullable(),
				group: z.string().nullable(),
				locked: z.boolean(),
				reusable: z.boolean(),
				layout: z.enum(reportElementLayoutEnum),
				config: reportConfigPartSchema.nullable()
			})
		),
		filtersToReportConfigs: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				reportElementConfigId: z.string(),
				filterId: z.string(),
				order: z.number()
			})
		),
		report: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				title: z.string(),
				group: z.string().nullable(),
				size: z.enum(pageSizeEnum),
				locked: z.boolean(),
				filterId: z.string().nullable()
			})
		),
		reportElement: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				title: z.string().nullable(),
				reportId: z.string(),
				rows: z.number(),
				cols: z.number(),
				order: z.number(),
				reportElementConfigId: z.string(),
				filterId: z.string().nullable()
			})
		),
		keyValueTable: z.array(
			z.object({
				key: z.string(),
				value: z.string()
			})
		),
		backup: z.array(
			z.object({
				...idColumn,
				...timestampColumns,
				title: z.string(),
				filename: z.string(),
				fileExists: z.boolean(),
				version: z.number(),
				restoreDate: z.date().optional().nullable(),
				compressed: z.boolean(),
				creationReason: z.string(),
				createdBy: z.string(),
				locked: z.boolean(),
				information: z.string()
			})
		)
	})
});

export type BackupSchemaRev09Type = z.infer<typeof backupSchemaRev09>;