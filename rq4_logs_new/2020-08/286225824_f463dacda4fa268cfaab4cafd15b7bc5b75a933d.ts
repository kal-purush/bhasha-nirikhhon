import { useMorfixContext } from './MorfixContext';
import { FieldContext } from './types';
import { get } from 'lodash';
import { useCallback } from 'react';

export const useDefaultFieldContext = <V>(name: string): FieldContext<V> => {
    const { values, setFieldValue } = useMorfixContext();

    const setValue = useCallback((value: V) => setFieldValue(name, value), [
        name
    ]);

    const setDeepValue = useCallback(
        <T>(deepName: string, value: T) =>
            setFieldValue(`${name}.${deepName}`, value),
        [name]
    );

    return [
        {
            value: get(values, name)
        },
        { setValue, setDeepValue }
    ];
};