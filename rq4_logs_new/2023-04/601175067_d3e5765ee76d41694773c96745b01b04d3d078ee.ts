/* eslint-disable @typescript-eslint/no-explicit-any */
import { useCallback } from 'react';
import { useRecoilState } from 'recoil';
import { modalState } from 'states/stateModal';

type OpenModalType = {
  title: string;
  content: JSX.Element | string;
  callback?: () => any;
};

export const useModal = () => {
  const [modalDataState, setModalDataState] = useRecoilState(modalState);

  const closeModal = useCallback(
    () =>
      setModalDataState((prev) => {
        return { ...prev, isOpen: false };
      }),
    [setModalDataState],
  );

  const openModal = useCallback(
    ({ title, content, callback }: OpenModalType) =>
      setModalDataState({
        isOpen: true,
        title,
        content,
        callBack: callback,
      }),
    [setModalDataState],
  );

  return { modalDataState, closeModal, openModal };
};