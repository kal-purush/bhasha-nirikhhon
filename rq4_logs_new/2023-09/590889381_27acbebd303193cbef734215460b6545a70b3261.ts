import {
  ChangeEvent,
  Dispatch,
  RefObject,
  SetStateAction,
  useRef,
} from 'react';

export const hiddenInputFeature = (
  setImg: Dispatch<SetStateAction<File | null>>,
  setPreviewImg: Dispatch<SetStateAction<string>>
): {
  inputRef: RefObject<HTMLInputElement>;
  onButtonClick: () => void;
  onChangeImageHandler: (e: ChangeEvent<HTMLInputElement>) => void;
} => {
  const inputRef = useRef<HTMLInputElement>(null);
  const onButtonClick = () => {
    inputRef.current?.click();
  };

  const onChangeImageHandler = (e: ChangeEvent<HTMLInputElement>) => {
    if (e.target.files![0]) {
      setImg(e.target.files![0]);
      setPreviewImg(URL.createObjectURL(e.target.files![0]));
      e.target.value = '';
    }
  };

  return { inputRef, onButtonClick, onChangeImageHandler };
};