import { createElement, PropsWithChildren, useCallback, useRef } from 'react';
import { DescendantContext } from './bones/DescendantContext';
import { DescendantManager } from './bones/DescendantManager';

function Descendant(props: PropsWithChildren<{}>) {
  const manager = useRef(new DescendantManager());

  const register = useCallback(
    (node: HTMLElement | null) => manager.current.register(node),
    [manager]
  );

  const unregister = useCallback(
    (node: HTMLElement | null) => manager.current.unregister(node),
    [manager]
  );

  const getIndex = useCallback(
    (node: HTMLElement | null): number => manager.current.getIndex(node),
    [manager]
  );

  return createElement(
    DescendantContext.Provider,
    { value: { register, unregister, getIndex } },
    props.children
  );
}

export default Descendant;