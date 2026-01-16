import { urlGenerator } from '$lib/routes';
import type { TagDropdownType } from '$lib/server/db/actions/tagActions';
import type { CategoryDropdownType } from '$lib/server/db/actions/categoryActions';
import type { BillDropdownType } from '$lib/server/db/actions/billActions';
import type { AccountDropdownType } from '$lib/server/db/actions/accountActions';
import type { ImportMappingDropdownType } from '$lib/server/db/actions/importMappingActions';
import type { BudgetDropdownType } from '$lib/server/db/actions/budgetActions';
import type { LabelDropdownType } from '$lib/server/db/actions/labelActions';
import { asyncDerived } from '@square/svelte-store';
import SuperJSON from 'superjson';
import { writable, type Writable } from 'svelte/store';
import { browser } from '$app/environment';
import { z } from 'zod';

const dropdownDataValidation = z.object({
	updatedTime: z.number(),
	storedTime: z.number(),
	data: z.array(z.any())
});

//Refresh Every Hour Even If No New Data Received
const maximumRefresh = 1000 * 60 * 60;

const getDropdownData = <T extends Record<string, any>[]>({
	url,
	prefix,
	updateValue
}: {
	url: string;
	prefix: string;
	updateValue: Writable<T>;
}) => {
	return async (newData: number[]): Promise<T> => {
		if (!browser) {
			const returnData = [] as unknown as T;
			return returnData;
		}
		const currentTime = newData[0];
		const nowTime = new Date().getTime();

		const dropdownStorageAddress = `${prefix}DropdownData`;

		const storedDropdown = window.localStorage.getItem(dropdownStorageAddress);

		const storedDropdownValidated = storedDropdown
			? dropdownDataValidation.safeParse(SuperJSON.parse(storedDropdown))
			: null;

		if (
			storedDropdownValidated &&
			!storedDropdownValidated.error &&
			storedDropdownValidated.data.data
		) {
			updateValue.set(storedDropdownValidated.data.data as unknown as T);
		}

		const storedDropdownValidated2 =
			!storedDropdownValidated || storedDropdownValidated.error
				? null
				: storedDropdownValidated.data;

		const needsRefresh =
			!storedDropdownValidated2 ||
			currentTime > storedDropdownValidated2.updatedTime ||
			storedDropdownValidated2.storedTime + maximumRefresh < nowTime;

		if (needsRefresh) {
			console.log(`Updating ${prefix} dropdown data...`);

			const data = await fetch(url);
			const dropdownText = await data.text();
			const dropdownData = SuperJSON.parse(dropdownText) as T;

			window.localStorage.setItem(
				dropdownStorageAddress,
				SuperJSON.stringify({ updatedTime: currentTime, storedTime: nowTime, data: dropdownData })
			);

			updateValue.set(dropdownData);

			return dropdownData;
		}

		return storedDropdownValidated2.data as unknown as T;
	};
};

// Tag Dropdown
export const tagDropdownTime = writable(0);
export const tagDropdownData = writable<TagDropdownType>([]);
export const tagDropdownAsyncData = asyncDerived(
	[tagDropdownTime],
	getDropdownData<TagDropdownType>({
		url: urlGenerator({ address: '/(loggedIn)/dropdowns/tags' }).url,
		prefix: 'tags',
		updateValue: tagDropdownData
	})
);

// Category Dropdown
export const categoryDropdownTime = writable(0);
export const categoryDropdownData = writable<CategoryDropdownType>([]);
export const categoryDropdownAsyncData = asyncDerived(
	[categoryDropdownTime],
	getDropdownData<CategoryDropdownType>({
		url: urlGenerator({ address: '/(loggedIn)/dropdowns/categories' }).url,
		prefix: 'categories',
		updateValue: categoryDropdownData
	})
);

// Bill Dropdown
export const billDropdownTime = writable(0);
export const billDropdownData = writable<BillDropdownType>([]);
export const billDropdownAsyncData = asyncDerived(
	[billDropdownTime],
	getDropdownData<BillDropdownType>({
		url: urlGenerator({ address: '/(loggedIn)/dropdowns/bills' }).url,
		prefix: 'bills',
		updateValue: billDropdownData
	})
);

// Account Dropdown
export const accountDropdownTime = writable(0);
export const accountDropdownData = writable<AccountDropdownType>([]);
export const accountDropdownAsyncData = asyncDerived(
	[accountDropdownTime],
	getDropdownData<AccountDropdownType>({
		url: urlGenerator({ address: '/(loggedIn)/dropdowns/accounts' }).url,
		prefix: 'accounts',
		updateValue: accountDropdownData
	})
);

// Import Mapping Dropdown
export const importMappingDropdownTime = writable(0);
export const importMappingDropdownData = writable<ImportMappingDropdownType>([]);
export const importMappingDropdownAsyncData = asyncDerived(
	[importMappingDropdownTime],
	getDropdownData<ImportMappingDropdownType>({
		url: urlGenerator({ address: '/(loggedIn)/dropdowns/importMappings' }).url,
		prefix: 'importMappings',
		updateValue: importMappingDropdownData
	})
);

// Budget Dropdown
export const budgetDropdownTime = writable(0);
export const budgetDropdownData = writable<BudgetDropdownType>([]);
export const budgetDropdownAsyncData = asyncDerived(
	[budgetDropdownTime],
	getDropdownData<BudgetDropdownType>({
		url: urlGenerator({ address: '/(loggedIn)/dropdowns/budgets' }).url,
		prefix: 'budgets',
		updateValue: budgetDropdownData
	})
);

// Label Dropdown
export const labelDropdownTime = writable(0);
export const labelDropdownData = writable<LabelDropdownType>([]);
export const labelDropdownAsyncData = asyncDerived(
	[labelDropdownTime],
	getDropdownData<LabelDropdownType>({
		url: urlGenerator({ address: '/(loggedIn)/dropdowns/labels' }).url,
		prefix: 'labels',
		updateValue: labelDropdownData
	})
);