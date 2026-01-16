import { useState, useEffect, useCallback } from "react";
import { getMinerColdkeys } from "../services/subnetsService";

import { useRollbar } from "@rollbar/react";
import { DataError } from "../utils/error";
import {
	MinerColdKey,
	MinerColdKeyPaginatedResponse,
	MinerColdKeyResponse,
} from "../model/subnet";

export function useMinerColdkeys(id: string): MinerColdKeyResponse {
	const rollbar = useRollbar();

	const [data, setData] = useState<MinerColdKey[]>([]);
	const [loading, setLoading] = useState<boolean>(true);
	const [error, setError] = useState<DataError>();

	const fetchData = useCallback(async () => {
		try {
			const limit = 100;

			let finished = false;
			let after: string | undefined = undefined;

			const result: MinerColdKey[] = [];
			while (!finished) {
				const coldkeys: MinerColdKeyPaginatedResponse = await getMinerColdkeys(
					{
						netUid: {
							equalTo: parseInt(id),
						},
					},
					"MINERS_COUNT_DESC",
					after,
					limit
				);
				result.push(...coldkeys.data);
				finished = !coldkeys.hasNextPage;
				after = coldkeys.endCursor;
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