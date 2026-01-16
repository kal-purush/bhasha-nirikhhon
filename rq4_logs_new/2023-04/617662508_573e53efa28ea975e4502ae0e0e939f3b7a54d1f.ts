import { useCallback } from "react";
import useAsync from "../useAsync";
import useToken from "../useToken";
import {
    type Construction,
    constructionApi,
} from "../../services/constructionApi";

export default function useConstructionDelete() {
    const token = useToken();

    const {
        loading: deleteConstructionIsLoading,
        error: deleteConstructionError,
        act: deleteConstruction,
    } = useAsync<number, Construction>(
        useCallback(
            async (id: number) =>
                await constructionApi.deleteConstructionById(token, id),
            [token]
        ),
        false
    );

    return {
        deleteConstructionIsLoading,
        deleteConstructionError,
        deleteConstruction,
    };
}