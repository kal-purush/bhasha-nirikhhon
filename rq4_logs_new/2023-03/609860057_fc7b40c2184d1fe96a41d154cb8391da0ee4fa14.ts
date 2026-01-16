import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { getBoardById } from "../store/board/boardSlice";
import { ColumnType } from "../types";
import { useAppDispatch } from "./hooks";

export const useColumnCollection = () => {
  //add error handling later
  const [columns, setColumns] = useState<ColumnType[] | undefined>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const { boardId } = useParams<{ boardId: string }>();

  const dispatch = useAppDispatch();

  useEffect(() => {
    if (!boardId) return;
    dispatch(getBoardById(boardId)).then((action) => {
      if (!action.payload) return;
      setColumns([...action.payload.board.columns]);
      setLoading(false);
    });
  }, [boardId, dispatch]);

  return { columns, setColumns, loading };
};