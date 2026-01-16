import { useReducer } from 'react';

export function wrapFetchHook<F extends (...args: any[]) => any>(hook: F) {
    function useFetchHook<S extends Parameters<F>[0]>(initialState: S): [S, React.Dispatch<Partial<S>>, ReturnType<F>] {
        const [state, dispatch] = useReducer((store: S, next: Partial<S>) => ({ ...store, ...next } as S), initialState);
        const response = hook(state);
        return [state, dispatch, response];
    }

    useFetchHook.displayName = `fetchHookWrapperFor_${typeof hook === 'function' ? hook?.name || 'anonymous_function' : 'arrow_function'}`;
    return useFetchHook;
}