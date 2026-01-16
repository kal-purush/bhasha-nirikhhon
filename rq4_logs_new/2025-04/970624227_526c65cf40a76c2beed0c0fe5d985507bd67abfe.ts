import { useQuery } from "@tanstack/react-query";
import _queryKeys from "./_queryKeys";
import axios from "axios";
import { FAQ } from "@/pages/home/_index";
import LaravelResponse from "@/types/utils/laravel";

export default function useQueryFaqs() {
  return useQuery({
    queryKey: _queryKeys.faqs,
    queryFn: async function () {
      return (await axios.get<LaravelResponse<FAQ[]>>("/faqs")).data;
    },
    staleTime: 5_000,
  });
}