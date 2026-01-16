import {atom, useRecoilState} from 'recoil';
import {InvoiceAccountDto} from '^types/invoiceAccount.type';
import {useEffect} from 'react';

const isSyncRunningAtom = atom({
    key: 'isSyncRunningAtom',
    default: false,
});

export function useSyncAccount(invoiceAccount: InvoiceAccountDto | null) {
    const [isSyncRunning, setIsSyncRunning] = useRecoilState(isSyncRunningAtom);

    useEffect(() => {
        if (!invoiceAccount) return;
        setIsSyncRunning(invoiceAccount.isSyncRunning);
    }, [invoiceAccount]);

    const startSync = () => {
        // setIsSyncRunning(true);
        alert('Not implemented yet');
    };

    return {isSyncRunning, startSync};
}