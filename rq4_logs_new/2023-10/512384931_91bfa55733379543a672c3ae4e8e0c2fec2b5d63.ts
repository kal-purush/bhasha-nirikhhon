import {useRecoilValue} from 'recoil';
import {orgIdParamState} from '^atoms/common';
import {CreditCardManager} from '^models/CreditCard/manager';
import {useEffect, useState} from 'react';
import {creditCardApi} from '^api/credit-crads.api';

export const useCreditCardsOfOrganization = (isShow: boolean) => {
    const orgId = useRecoilValue(orgIdParamState);
    const [CreditCard, setCreditCardManager] = useState<CreditCardManager>();

    useEffect(() => {
        if (!isShow) return;
        if (!orgId || isNaN(orgId)) return;

        creditCardApi.index(orgId, {itemsPerPage: 0}).then((res) => {
            setCreditCardManager(CreditCardManager.init(res.data.items));
        });
    }, [isShow, orgId]);

    return {CreditCard};
};