import { useState, useEffect, useCallback } from "react";
import { getNeuronRegCostHistory } from "../services/subnetsService";

import { useRollbar } from "@rollbar/react";
import { DataError } from "../utils/error";
import {
	NeuronRegCostHistory,
	NeuronRegCostHistoryPaginatedResponse,
	NeuronRegCostHistoryResponse,
} from "../model/subnet";

export function useNeuronRegCostHistory(
	id: string
): NeuronRegCostHistoryResponse {
	const rollbar = useRollbar();

	const [data, setData] = useState<NeuronRegCostHistory[]>([]);
	const [loading, setLoading] = useState<boolean>(true);
	const [error, setError] = useState<DataError>();

	const fetchData = useCallback(async () => {
		try {
			const limit = 100;

			let finished = false;
			let after: string | undefined = undefined;

			const result: NeuronRegCostHistory[] = [];
			while (!finished) {
				const regCost: NeuronRegCostHistoryPaginatedResponse =
					await getNeuronRegCostHistory(
						{
							netUid: {
								equalTo: parseInt(id),
							},
						},
						"HEIGHT_ASC",
						after,
						limit
					);
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
	};
}