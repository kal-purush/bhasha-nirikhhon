import Router from "next/router";

import { useMutation } from "react-query";
import toast from "react-hot-toast";

import { instance } from "@/lib/axios";

import { PostData } from "@/types/post";

const useCreatePost = () =>
  useMutation((data: PostData) => instance.post(`/posts`, data), {
    onSuccess: ({ data }) => {
      Router.push(`/p/${data.slug}`);
      toast.success("Your post was created successfully!");
    },
    onError: (err) => {
      if ("message" in (err as Error)) {
        toast.error((err as Error).message);
      }
    },
  });

export default useCreatePost;