import {useEffect} from "react";

export function useOnChanged(trigger: () => any, fn: () => void) {
    useEffect(fn, [trigger()]);
}