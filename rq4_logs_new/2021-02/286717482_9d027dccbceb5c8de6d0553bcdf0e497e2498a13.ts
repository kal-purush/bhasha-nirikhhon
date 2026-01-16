import React, { useEffect, useState } from 'react';

import { list } from '../../../api';

export type Category = {
    _id: string,
  title: string,
}

export function useCategories() {
  const [data, setData] = useState<Category[]>([]);
  const [isLoading, setLoading] = useState<boolean>(false);
  const [isError, setError] = useState<boolean>(false);

  useEffect(() => {
    const fetchCategories = async () => {
      setError(false);
      setLoading(true);

      try {
        const result = await list('/categories');
        setData(result);
      } catch (error) {
        setError(true);
      }

      setLoading(false);
    };

    fetchCategories();
  }, []);

  return [data, isLoading, isError];
}