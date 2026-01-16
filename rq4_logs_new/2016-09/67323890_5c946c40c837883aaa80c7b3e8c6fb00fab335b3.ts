import { createSelector } from 'reselect';
import { RootState, Item, PurchaseItem } from '../reducers';

const getPriceList = (state: RootState) => state.app.priceList;
const getPurchaseItems = (state: RootState) => state.app.purchaseItems;

export const getVisiblePriceList = createSelector<RootState, Item[], Item[], PurchaseItem[]>(
    getPriceList, getPurchaseItems,
    (priceList, purchaseItems) => {
        const addedIds = purchaseItems.map(x => x.id);
        return priceList.filter(x => !addedIds.find(y => x.id === y));
    }
)

export interface Option {
    label?: string;
    text: string;
    disabled?: boolean;
    static?: boolean;
    value?: string
}

export const getVisibleOptions = createSelector<RootState, Option[], Item[]>(
    getVisiblePriceList,
    (priceList) => {
        const priceOptions: Option[] = priceList.map(x => {
            return {
                text: `${x.name} ${x.itemId} ${x.menu}`,
                value: x.id
            } as Option;
        });
        return priceOptions;
    }
)

export interface PurchaseDetailItem extends PurchaseItem, Item {
}

export const getPurchaseDetailItems = createSelector<RootState, PurchaseDetailItem[], Item[], PurchaseItem[]>(
    getPriceList, getPurchaseItems,
    (priceList, purchaseItems) => {
        return purchaseItems.map(x => {
            const item = priceList.find(y => y.id === x.id);
            return <PurchaseDetailItem>Object.assign({}, item, x);
        });
    }
)