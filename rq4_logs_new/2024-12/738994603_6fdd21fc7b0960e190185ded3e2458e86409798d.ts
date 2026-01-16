import { useState } from "react";
import {
  type DocumentGraph,
  useGraphDocumentDispatch,
  useGraphDocumentState,
} from "contexts/graph-document-context";
import { getAllGraphs } from "db/indexedDB";

export const useGetAll = () => {
  const state = useGraphDocumentState();
  const dispatch = useGraphDocumentDispatch();
  const [loading, setLoading] = useState(false);

  const getAllGraphDocuments = async () => {
    setLoading(true);
    const graphs: DocumentGraph[] = await getAllGraphs();
    setLoading(false);
    dispatch({ type: "SET_GRAPHS", payload: graphs });
  };

  return {
    state,
    loading,
    getAllGraphDocuments,
  };
};