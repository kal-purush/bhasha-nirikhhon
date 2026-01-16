import { useEffect, useState } from "react";

const useSelected = checkedList => {
  const [selStr, setselStr] = useState("");
  useEffect(() => {
    setselStr(checkedList.join("/"));
  }, [checkedList]);

  return [selStr];
};

export default useSelected;