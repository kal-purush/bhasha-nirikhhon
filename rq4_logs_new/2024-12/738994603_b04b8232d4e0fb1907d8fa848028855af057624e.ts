import {
  useGraphDocumentDispatch,
  useGraphDocumentState,
} from "contexts/graph-document-context";
import { useGraph } from "contexts/graphContext";
import { getAllGraphs, updateGraph } from "db/indexedDB";
import { useEffect, useState } from "react";

export const useUpdate = () => {
  const state = useGraphDocumentState();
  const dispatch = useGraphDocumentDispatch();
  const [pending, setPending] = useState(false);
  const { graph } = useGraph();

  useEffect(() => {
    const fetchGraphs = async () => {
      const graphs = await getAllGraphs();
      dispatch({ type: "SET_GRAPHS", payload: graphs });
    };

    fetchGraphs();
  }, [dispatch]);

  const updateGraphDocument = async (name: string) => {
    setPending(true);
    await updateGraph(name, graph);
    setPending(false);
    dispatch({ type: "ADD_GRAPH", payload: { name, data: graph } });
  };

  return {
    state,
    pending,
    updateGraphDocument,
  };
};