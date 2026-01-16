import { useQuery } from '@tanstack/react-query';
import { UserData } from '@/types/mentor-search/user-data';
import { useUser } from '@auth0/nextjs-auth0/client';

const fetchRecommendedMentors = async (userId: string): Promise<UserData[]> => {
  const response = await fetch(`/api/users?recommended=true`, {
    headers: {
      'X-User-Id': userId,
      'Content-Type': 'application/json',
    },
  });
  if (!response.ok) throw new Error('Failed to fetch recommended mentors');
  return response.json();
};

export const useRecommendedMentors = () => {
  const { user } = useUser();
  const queryKey = ['recommended'];
  const { data, isLoading, error } = useQuery<UserData[], Error>({
    queryKey,
    queryFn: () => fetchRecommendedMentors(user?.sub ?? ''),
    enabled: !!user?.sub,
  });

  return {
    recommendedMentors: data ?? [],
    isLoading,
    error,
  };
};