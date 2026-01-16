import { useMutation, useQuery } from "@tanstack/react-query";
import axios from "axios";

const createNews = (
  news: {
    title: string;
    data: string;
    body: string;
    image: string;
  }[]
) => {
  return axios.post("/api/create-news", news);
};

const getAllNews = () => {
  return axios.get("/api/get-all-news");
};

const getNews = (path: string) => {
  return axios.get(`/api/get-news?title=${path}`);
};

export const useCreateNews = (onSuccess: any, onError: any) => {
  return useMutation(createNews, {
    onSuccess,
    onError,
  });
};

export const useGetNews = (onSuccess: any, onError: any, path: string) => {
  return useQuery(["news", path], () => getNews(path), {
    onSuccess,
    onError,
  });
};

export const useGetAllNews = (onSuccess: any, onError: any) => {
  return useQuery(["all-news"], getAllNews, {
    onSuccess,
    onError,
  });
};