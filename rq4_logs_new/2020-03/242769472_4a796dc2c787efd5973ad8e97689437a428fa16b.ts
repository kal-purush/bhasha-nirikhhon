import * as React from 'react';

type dataID = string | number;

interface UseCheckboxProps {
  identifier: dataID;
  data: Array<object>;
}

interface AssocArrayType {
  [key: string]: boolean;
}

interface UseCheckboxReturnObj {
  allData: Array<string>;
  allCheckedData: Array<string>;
  selectAllData: (b: boolean) => void;
  setSelectedData: (id: dataID, b: boolean) => void;
}

const useCheckbox = ({
  identifier,
  data,
}: UseCheckboxProps): UseCheckboxReturnObj => {
  const [assocArray, setAssocArray] = React.useState<AssocArrayType>(() => {
    const initialArray: AssocArrayType = {};
    data.forEach(d =>
      Object.entries(d).forEach(([key, value]) => {
        if (key === identifier) initialArray[value] = false;
      })
    );
    return initialArray;
  });

  React.useEffect(() => {
    setAssocArray(() => {
      const initialArray: AssocArrayType = {};
      data.forEach(d =>
        Object.entries(d).forEach(([key, value]) => {
          if (key === identifier) initialArray[value] = false;
        })
      );
      return initialArray;
    });
  }, [identifier, data]);

  // Array of all keys
  const allData = Object.keys(assocArray);

  // Array of selected keys (true)
  const allCheckedData = Object.keys(assocArray).filter(
    key => assocArray[key] === true
  );

  // Set id to boolean
  const setSelectedData = (id: dataID, b: boolean): void =>
    setAssocArray(prevAssocArray => {
      return { ...prevAssocArray, [id]: b };
    });

  // Select all or none with passed bool
  const selectAllData = (b: boolean): void =>
    setAssocArray(prevAssocArray => {
      const l: AssocArrayType = {};
      Object.keys(prevAssocArray).forEach(k => {
        l[k] = b;
      });
      return l;
    });

  return {
    allData,
    allCheckedData,
    selectAllData,
    setSelectedData,
  };
};

export default useCheckbox;