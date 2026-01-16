import { QueryDocumentSnapshot } from 'firebase/firestore';
import { useEffect, useMemo, useState } from 'react';

import { PlanGroupsListInterface } from 'spelieve-common/lib/Interfaces/Itinerary/ICT021';

export const IMC03103PlanGroupsEditController = ({
	planGroupsDoc,
}: {
	planGroupsDoc: QueryDocumentSnapshot<PlanGroupsListInterface>;
}) => {
	const planGroups = planGroupsDoc.data();

	const [isMounted, setIsMounted] = useState<boolean>(false);

	useEffect(() => {
		setIsMounted(true);
	}, []);

	const draxItemList = useMemo(() => {
		let representativeFounded = false;
		return planGroups.plans.map((planID, index) => {
			const beforeAfterRepresentativeType: 'after' | 'representative' | 'before' = (() => {
				if (representativeFounded) {
					return 'after';
				}
				if (planID === planGroups.representativePlanID) {
					representativeFounded = true;
					return 'representative';
				}
				return 'before';
			})();
			const dependentPlanID = (() => {
				if (beforeAfterRepresentativeType === 'before') {
					return planGroups.plans[index + 1];
				}
				if (beforeAfterRepresentativeType === 'after') {
					return planGroups.plans[index - 1];
				}
				return planID;
			})();
			return {
				planID,
				index,
				beforeAfterRepresentativeType,
				dependentPlanID,
			};
		});
	}, [planGroups.plans, planGroups.representativePlanID]);

	return { isMounted, draxItemList };
};