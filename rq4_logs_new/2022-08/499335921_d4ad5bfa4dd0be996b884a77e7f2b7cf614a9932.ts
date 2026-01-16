import { renderHook, act } from '@testing-library/react';
import { useLoader } from './useLoader';
import { STATUS } from '../../types';

describe('useLoader', () => {
	describe('initialization', () => {
		test('loadingStatus should be INIT', () => {
			const defaultStatus = STATUS.INIT;
			const { result } = renderHook(() => useLoader());

			expect(result.current.loadingStatus.status).toEqual(defaultStatus);
		});

		test('data should be null', () => {
			const defaultDataState = null;
			const { result } = renderHook(() => useLoader());

			expect(result.current.data).toEqual(defaultDataState);
		});

		test('passing defaultStatus should change the default loadingStatus', () => {
			const defaultStatus = STATUS.REQUESTING;
			const { result } = renderHook(() => useLoader(defaultStatus));

			expect(result.current.loadingStatus.status).toEqual(defaultStatus);
		});

		test('passing defaultDataState should change the default data', () => {
			const defaultDataState = {};
			const { result } = renderHook(() => useLoader(undefined, defaultDataState));

			expect(result.current.data).toEqual(defaultDataState);
		});
	});

	describe('setLoading', () => {
		test('should set loadingStatus to REQUESTING', () => {
			const { result } = renderHook(() => useLoader());
			act(() => {
				result.current.setLoading();
			});

			expect(result.current.loadingStatus.status).toEqual(STATUS.REQUESTING);
		});

		test('after success, setLoading with nothing passed should preserve data', () => {
			const { result } = renderHook(() => useLoader());
			const response = 'response';
			act(() => {
				result.current.setSuccess(response);
			});
			act(() => {
				result.current.setLoading();
			});

			expect(result.current.data).toEqual(response);
		});

		test('isRefetch: true should change loadingStatus to REFETCH', () => {
			const { result } = renderHook(() => useLoader());
			act(() => {
				result.current.setLoading(true);
			});

			expect(result.current.loadingStatus.status).toEqual(STATUS.REFETCH);
		});

		test('after success, setLoading with shouldClearData: true should clear data', () => {
			const { result } = renderHook(() => useLoader());
			const response = 'response';
			act(() => {
				result.current.setSuccess(response);
			});
			act(() => {
				result.current.setLoading(false, true);
			});

			expect(result.current.data).toEqual(null);
		});
	});

	describe('setSuccess', () => {
		test('should set loadingStatus to SUCCESS', () => {
			const { result } = renderHook(() => useLoader());
			act(() => {
				result.current.setSuccess();
			});

			expect(result.current.loadingStatus.status).toEqual(STATUS.SUCCESS);
		});

		test('passing the response should set data with the response', () => {
			const { result } = renderHook(() => useLoader());
			const response = 'response';
			act(() => {
				result.current.setSuccess(response);
			});

			expect(result.current.data).toEqual(response);
		});

		test('passing no response should set data to null', () => {
			const { result } = renderHook(() => useLoader());
			act(() => {
				result.current.setSuccess();
			});

			expect(result.current.loadingStatus.status).toEqual(STATUS.SUCCESS);
			expect(result.current.data).toEqual(null);
		});
	});

	describe('onError', () => {
		test('should set loadingStatus to FAILURE', () => {
			const { result } = renderHook(() => useLoader());
			act(() => {
				result.current.setError();
			});

			expect(result.current.loadingStatus.status).toEqual(STATUS.FAILURE);
		});

		test('after success, passing error should set error', () => {
			const { result } = renderHook(() => useLoader());
			const error = 'error';
			act(() => {
				result.current.setError(error);
			});

			expect(result.current.loadingStatus.reason).toEqual(error);
		});

		test('after success, error should preserve data', () => {
			const { result } = renderHook(() => useLoader());
			const response = 'response';
			const error = 'error';
			act(() => {
				result.current.setSuccess(response);
			});
			act(() => {
				result.current.setError(error);
			});

			expect(result.current.data).toEqual(response);
		});

		test("after success, passing no error should set error to ''", () => {
			const { result } = renderHook(() => useLoader());
			const response = 'response';
			act(() => {
				result.current.setSuccess(response);
			});
			act(() => {
				result.current.setError();
			});

			expect(result.current.loadingStatus.reason).toEqual('');
		});

		test('after success, shouldClearData: true should clear data', () => {
			const { result } = renderHook(() => useLoader());
			const response = 'hello';
			const error = 'error';
			act(() => {
				result.current.setSuccess(response);
			});
			act(() => {
				result.current.setError(error, true);
			});

			expect(result.current.data).toEqual(null);
		});
	});
});