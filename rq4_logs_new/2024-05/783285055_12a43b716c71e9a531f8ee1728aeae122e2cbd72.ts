'use client';

import { useCallback, useState } from 'react';

export interface UseDisclosureProps {
  isOpen?: boolean;
  onOpen?: () => void;
  onChange?: (isOpen: boolean | undefined) => void;
  onClose?: () => void;
}

export default function useDisclosure({ ...props }: UseDisclosureProps = {}) {
  const {
    isOpen: isOpenProp,
    onOpen: onOpenProp,
    onClose: onCloseProp,
  } = props;
  const [isOpen, setIsOpen] = useState<boolean>(false);
  const isControlled = isOpenProp !== undefined;

  const onOpen = useCallback(() => {
    if (!isControlled) {
      setIsOpen(true);
    }
    onOpenProp?.();
  }, [isControlled, onOpenProp, setIsOpen]);

  const onClose = useCallback(() => {
    if (!isControlled) {
      setIsOpen(false);
    }
    onCloseProp?.();
  }, [isControlled, setIsOpen, onCloseProp]);

  const onOpenChange = useCallback(() => {
    const action = isOpen ? onClose : onOpen;

    action();
  }, [isOpen, onOpen, onClose]);

  return {
    isOpen: !!isOpen,
    onOpen,
    onOpenChange,
    onClose,
  };
}