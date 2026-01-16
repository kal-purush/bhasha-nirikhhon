import { useQuery } from "@tanstack/react-query";
import axios from "axios";
import { LIMIT_LIST_SCHOOL } from "../../../constants/schoolLimit";
import { useEffect, useState } from "react";
import { useInView } from "react-intersection-observer";
import { UniversityData } from "../DynamicPagination/types";

export const usePagination = () => {
  const [currentPage, setCurrentPage] = useState(1);

  const fetchUniversities = async (): Promise<UniversityData[]> => {
    const offset = (currentPage - 1) * LIMIT_LIST_SCHOOL;
    const response = await axios.get<UniversityData[]>(
      `http://universities.hipolabs.com/search?offset=${offset}&limit=${LIMIT_LIST_SCHOOL}`,
    );
    return response.data;
  };

  const { data, isLoading, error } = useQuery<UniversityData[], Error>({
    queryKey: ["universitiesData"],
    queryFn: fetchUniversities,
  });

  const { ref, inView } = useInView({
    threshold: 1.0,
  });

  useEffect(() => {
    if (inView) {
      setCurrentPage((prev) => prev + 1);
    }
  }, [inView]);

  return { data, isLoading, error, ref };
};