import { useState, useEffect, useCallback } from "react";
import {
	getSubnetRegCostHistory,
	getSubnets,
} from "../services/subnetsService";

import { useRollbar } from "@rollbar/react";
import { DataError } from "../utils/error";
import {
	SubnetRegCostHistory,
	SubnetRegCostHistoryPaginatedResponse,
	SubnetRegCostHistoryResponse,
} from "../model/subnet";

export function useSubnetRegCostHistory(): SubnetRegCostHistoryResponse {
	const rollbar = useRollbar();

	const [data, setData] = useState<SubnetRegCostHistory[]>([]);
	const [ids, setIds] = useState<number[]>([]);
	const [loading, setLoading] = useState<boolean>(true);
	const [error, setError] = useState<DataError>();

	const fetchData = useCallback(async () => {
		try {
			const subnets = await getSubnets(undefined, "EMISSION_DESC", {
				limit: 12,
			});
			const subnetIds = subnets.data.slice(0, 12).map((x) => x.netUid);
			setIds(subnetIds);

			const limit = 100;

			let finished = false;
			let after: string | undefined = undefined;

			const result: SubnetRegCostHistory[] = [];
			while (!finished) {
				const regCost: SubnetRegCostHistoryPaginatedResponse =
					await getSubnetRegCostHistory(undefined, "HEIGHT_ASC", after, limit);
				result.push(...regCost.data);
				finished = !regCost.hasNextPage;
				after = regCost.endCursor;
			}
			setData(result);
		} catch (e) {
			if (e instanceof DataError) {
				rollbar.error(e);
				setError(e);
			} else {
				throw e;
			}
		}

		setLoading(false);
	}, []);

	useEffect(() => {
		setData([]);
		setError(undefined);
		setLoading(true);
		fetchData();
	}, [fetchData]);

	return {
		loading,
		error,
		data,
		ids,
	};
}