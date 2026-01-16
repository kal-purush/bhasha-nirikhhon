import { useDispatch, useSelector } from "react-redux";
import { RootState } from "../redux/rootReducer";
import { addArticle } from "../redux/slices/article";
import { IArticle } from "../types";

function useArticle() {
  const dispatch = useDispatch();
  const article = useSelector<RootState, RootState["article"]>(
    (state) => state.article
  );

  return {
    ...article,
    addArticle: async (data: IArticle) => {
      dispatch(await addArticle(data));
    },
  };
}

export default useArticle;