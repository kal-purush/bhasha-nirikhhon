import type { UpdateJournalSchemaType } from '$lib/schema/journalSchema';

const keysToBeUndefined = [
	'date',
	'description',
	'amount',
	'tagId',
	'tagTitle',
	'billId',
	'billTitle',
	'budgetId',
	'budgetTitle',
	'categoryId',
	'categoryTitle',
	'accountId',
	'accountTitle',
	'otherAccountId',
	'otherAccountTitle'
] as const satisfies (keyof UpdateJournalSchemaType)[];

const keysToBeFalse = [
	'tagClear',
	'billClear',
	'budgetClear',
	'categoryClear',
	'setReconciled',
	'clearReconciled',
	'setDataChecked',
	'clearDataChecked',
	'setComplete',
	'clearComplete',
	'setLinked',
	'clearLinked'
] as const satisfies (keyof UpdateJournalSchemaType)[];

export const checkUpdateLabelsOnly = (update: UpdateJournalSchemaType) => {
	for (const key of keysToBeUndefined) {
		if (update[key] !== undefined) {
			return false;
		}
	}

	for (const key of keysToBeFalse) {
		if (update[key] !== false) {
			return false;
		}
	}

	return true;
};