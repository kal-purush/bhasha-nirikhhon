import { useCallback, useEffect, useRef, useState } from "react";

import { AxiosResponse } from "axios";

import useAxios from "../hooks/useAxios";
import { useToast } from "../contexts/ToastContext";

import { CommentType } from "../lib/types";

const useComments = (projectId: number, inViewPort: boolean) => {
    const [comments, setComments] = useState<CommentType[]>([]);
    const [loadingComments, setLoading] = useState(true);

    const [loadingSubmit, setLoadingSubmit] = useState(false);

    const isFirstFetch = useRef(false);

    const axiosInstance = useAxios();
    const { toast } = useToast();

    const url = `/projects/${projectId}/comments`;

    useEffect(() => {
        if (!inViewPort || isFirstFetch.current) return;

        fetchComments();
        isFirstFetch.current = true;
    }, [inViewPort]);

    const fetchComments = async () => {
        setLoading(true);
        try {
            const response: AxiosResponse = await axiosInstance.get(url);
            setComments(response.data.data);
        } catch (e: any) {
            console.log("Error occured in fetchComments", e);
        } finally {
            setLoading(false);
        }
    };

    const postComment = useCallback(async (text: string) => {
        setLoadingSubmit(true);
        try {
            const response = await axiosInstance.post(url, { text });
            toast("Comment Submitted Successfully", "success");
            setComments((prev) => [...prev, response.data.comment]);
        } catch (e: any) {
            console.log("Error occured in postComment", e);
        } finally {
            setLoadingSubmit(false);
        }
    }, []);

    const deleteComment = useCallback(async (commentId: number) => {
        try {
            const deleteUrl = url + `/${commentId}`;
            await axiosInstance.delete(deleteUrl);
            toast("Comment Deleted Successfully", "success");
            setComments((prev) => prev.filter((comment) => comment.id !== commentId));
        } catch (e: any) {
            console.log("Error occured in deleteComment", e);
        }
    }, []);

    return { comments, loadingComments, loadingSubmit, postComment, deleteComment };
};

export default useComments;