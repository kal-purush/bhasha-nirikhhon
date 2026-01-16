import { useCallback } from "react";
import useAsync from "../useAsync";
import useToken from "../useToken";
import {
    constructionApi,
    type getConstructionsResponse,
} from "../../services/constructionApi";

export default function useConstructions() {
    const token = useToken();

    const {
        data: constructions,
        loading: constructionsIsLoading,
        error: constructionsError,
        act: getConstructions,
    } = useAsync<string, getConstructionsResponse>(
        useCallback(
            async () => await constructionApi.getConstructions(token),
            [token]
        )
    );

    return {
        constructions,
        constructionsIsLoading,
        constructionsError,
        getConstructions,
    };
}