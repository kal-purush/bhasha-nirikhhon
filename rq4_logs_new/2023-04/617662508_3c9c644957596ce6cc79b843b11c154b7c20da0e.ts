import { useCallback } from "react";
import useAsync from "../useAsync";
import useToken from "../useToken";
import {
    type Construction,
    constructionApi,
} from "../../services/constructionApi";

export default function useConstruction() {
    const token = useToken();

    const {
        data: construction,
        loading: constructionIsLoading,
        error: constructionError,
        act: getConstruction,
    } = useAsync<number, Construction>(
        useCallback(
            async (id: number) =>
                await constructionApi.getConstructionById(token, id),
            [token]
        ),
        false
    );

    return {
        construction,
        constructionIsLoading,
        constructionError,
        getConstruction,
    };
}