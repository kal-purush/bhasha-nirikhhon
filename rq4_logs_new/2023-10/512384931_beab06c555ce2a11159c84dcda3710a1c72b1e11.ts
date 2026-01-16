import {atom, useRecoilState} from 'recoil';
import {InvoiceAccountDto} from '^types/invoiceAccount.type';
import {invoiceAccountApi} from '^api/invoiceAccount.api';
import {SubscriptionDto} from '^types/subscription.type';
import {useEffect} from 'react';
import {subscriptionApi} from '^api/subscription.api';

const currentInvoiceAccountState = atom<InvoiceAccountDto | null>({
    key: 'currentInvoiceAccountState',
    default: null,
});

const currentInvoiceAccountLoadingState = atom<boolean>({
    key: 'currentInvoiceAccountLoadingState',
    default: false,
});

export const useCurrentInvoiceAccount = () => {
    const [currentInvoiceAccount, setCurrentInvoiceAccount] = useRecoilState(currentInvoiceAccountState);
    const [isLoading, setIsLoading] = useRecoilState(currentInvoiceAccountLoadingState);

    const loadCurrentInvoiceAccount = (organizationId: number, id: number) => {
        setIsLoading(true);
        const request = invoiceAccountApi.show(organizationId, id);
        request.then((res) => setCurrentInvoiceAccount(res.data));
        request.finally(() => setIsLoading(false));
    };

    return {currentInvoiceAccount, loadCurrentInvoiceAccount, isLoading};
};

const subscriptionsOfAccountAtom = atom<SubscriptionDto[]>({
    key: 'subscriptionsOfAccountAtom',
    default: [],
});

export const useCurrentSubscriptions = () => {
    const [subscriptions, setSubscriptions] = useRecoilState(subscriptionsOfAccountAtom);

    const loadCurrentSubscriptions = (invoiceAccountId: number) => {
        subscriptionApi
            .index({where: {invoiceAccountId}, itemsPerPage: 0})
            .then((res) => setSubscriptions(res.data.items));
    };

    return {subscriptions, loadCurrentSubscriptions};
};