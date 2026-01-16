import { useState } from "react";

const useInput = (initValue: any) => {
  const [value, setValue] = useState(initValue);

  const reset = () => setValue(initValue);

  const attributeObj = {
    value,
    onChange: (e: React.ChangeEvent<HTMLInputElement>) => {
      setValue(e.target.value);
    },
  };

  return [value, reset, attributeObj];
};

export default useInput;