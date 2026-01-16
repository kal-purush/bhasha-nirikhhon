import { useCallback } from "react";
import useAsync from "../useAsync";
import useToken from "../useToken";
import {
    type PostConstructionResponse,
    constructionApi,
} from "../../services/constructionApi";
import { type ConstructionForm } from "../../pages";

export default function usePostConstruction() {
    const token = useToken();

    const {
        data: construction,
        loading: postConstructionIsLoading,
        error: postConstructionError,
        act: postConstruction,
    } = useAsync<ConstructionForm, PostConstructionResponse>(
        useCallback(
            async (body) => await constructionApi.postConstruction(token, body),
            [token]
        ),
        false
    );

    return {
        construction,
        postConstructionIsLoading,
        postConstructionError,
        postConstruction,
    };
}