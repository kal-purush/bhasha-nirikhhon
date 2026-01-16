import { useState } from 'react';
import { getErrorMessage } from 'utils/errors';

type AsyncF<P, R> = (...params: P[]) => Promise<R>;

export const useAsync = <P, R>(
	func: AsyncF<P, R | undefined>,
	onSucess?: (data: R) => void,
	onError?: (error: unknown) => void
) => {
	const [loading, setLoading] = useState(false);
	const [error, setError] = useState<string>();
	const [data, setData] = useState<R>();

	const handler: typeof func = async (...params) => {
		setLoading(true);
		try {
			const response = (await func(...params)) as R;
			setData(response);
			if (onSucess) onSucess(response);
			return response;
		} catch (e) {
			if (onError) onError(e);
			const errorMessage = getErrorMessage(e);
			setError(errorMessage);
		} finally {
			setLoading(false);
		}
		return undefined;
	};
	return { data, handler, loading, error };
};