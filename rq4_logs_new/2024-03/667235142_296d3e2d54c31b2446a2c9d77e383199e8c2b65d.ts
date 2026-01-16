import { useState, useEffect, useCallback } from "react";
import { getSubnetHistory } from "../services/subnetsService";

import { useRollbar } from "@rollbar/react";
import { DataError } from "../utils/error";
import {
	SubnetHistory,
	SubnetHistoryPaginatedResponse,
	SubnetHistoryResponse,
} from "../model/subnet";

export function useSubnetHistory(subnetId: string): SubnetHistoryResponse {
	const rollbar = useRollbar();

	const [data, setData] = useState<SubnetHistory[]>([]);
	const [loading, setLoading] = useState<boolean>(true);
	const [error, setError] = useState<DataError>();

	const fetchData = useCallback(async () => {
		try {
			const limit = 100;

			let finished = false;
			let after: string | undefined = undefined;

			const result: SubnetHistory[] = [];
			while (!finished) {
				const stats: SubnetHistoryPaginatedResponse = await getSubnetHistory(
					{
						subnetId: {
							equalTo: parseInt(subnetId),
						},
					},
					"HEIGHT_ASC",
					undefined,
					after,
					limit
				);
				result.push(...stats.data);
				finished = !stats.hasNextPage;
				after = stats.endCursor;
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
		ids: [],
	};
}