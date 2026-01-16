import { useQuery } from '@tanstack/react-query';
import useAuth from './useAuth';
import { User } from '@/types/user.type';
import { getUserByUID, getUsersByUIDs } from '@/services/user.service';
import { getSkills } from '@/services/skill.service';
import { Skill } from '@/types/skill.type';

const useInvitations = () => {
  const { user } = useAuth();

  return useQuery<User[]>({
    queryKey: ['invitations', user?.id],
    queryFn: async () => {
      if (!user?.id) {
        throw new Error('User not logged in');
      }

      // get the current user data
      const userData = await getUserByUID(user.id);

      if (!userData) {
        throw new Error('User data not found');
      }

      const requestedIds = (userData.requestConnections ?? []) as string[];

      if (requestedIds.length === 0) {
        return [];
      }

      // get all skills to create a mapping
      const skills = await getSkills();
      const skillsMap = skills.reduce(
        (map, skill) => {
          map[skill.id] = skill;
          return map;
        },
        {} as Record<string, Skill>,
      );

      // get the connected users data
      const requestedUsers = await getUsersByUIDs(requestedIds as string[]);

      // map skills to their names
      return requestedUsers.map((requestedUser) => {
        // map teach skills
        const teachSkills = Array.isArray(requestedUser.teach)
          ? (requestedUser.teach as string[]).map((skillId) => skillsMap[skillId]?.name || 'Unknown Skill')
          : [];

        // map learn skills
        const learnSkills = Array.isArray(requestedUser.learn)
          ? (requestedUser.learn as string[]).map((skillId) => skillsMap[skillId]?.name || 'Unknown Skill')
          : [];

        return {
          ...requestedUser,
          teach: teachSkills,
          learn: learnSkills,
        };
      });
    },
    enabled: !!user?.id,
    refetchOnWindowFocus: true,
    refetchOnMount: true,
    staleTime: 0,
  });
};

export default useInvitations;