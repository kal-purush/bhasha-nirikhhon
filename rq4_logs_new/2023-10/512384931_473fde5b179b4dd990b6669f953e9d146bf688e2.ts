import {useEffect} from 'react';
import {useRecoilState} from 'recoil';
import {getCreditCards} from '^api/credit-crads.api';
import {cardListResultAtom} from '^atoms/cards.atom';
import {orgIdParamState, useRouterIdParamState} from '^atoms/common';

export const useCards = () => {
    const orgId = useRouterIdParamState('orgId', orgIdParamState);
    const [result, setResult] = useRecoilState(cardListResultAtom);

    useEffect(() => {
        if (!orgId) return;
        getCreditCards(orgId).then((res) => setResult(res.data));
    }, [orgId]);

    return {result};
};