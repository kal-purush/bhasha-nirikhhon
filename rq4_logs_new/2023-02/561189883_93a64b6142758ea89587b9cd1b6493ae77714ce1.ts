import React, { useState } from 'react';
import { createPortal } from 'react-dom';
import { useIsomorphicLayoutEffect } from '../hooks/useIsomorphicLayoutEffect';

// Based on https://github.com/mantinedev/mantine/blob/master/src/mantine-core/src/Portal/Portal.tsx

type Props = {
  children: React.ReactElement;
};
const Portal = (props: Props) => {
  const { children } = props;

  const [mounted, setMounted] = useState(false);

  const rootDom = document.getElementById('root');

  useIsomorphicLayoutEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted || !rootDom) {
    return null;
  }

  return createPortal(children, rootDom);
};

export default Portal;