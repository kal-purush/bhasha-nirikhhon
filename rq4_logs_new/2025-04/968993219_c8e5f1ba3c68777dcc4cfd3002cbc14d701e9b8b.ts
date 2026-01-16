import { useQuery } from '@tanstack/react-query';
import useAuth from './useAuth';
import { getUserByUID, getUsersByUIDs } from '@/services/user.service';
import { User } from '@/types/user.type';
import { getSkills } from '@/services/skill.service';
import { Skill } from '@/types/skill.type';

const useConnections = () => {
  const { user } = useAuth();

  return useQuery<User[]>({
    queryKey: ['connections', user?.uid],
    queryFn: async () => {
      if (!user?.uid) {
        throw new Error('User not logged in');
      }

      // get the current user data
      const userData = await getUserByUID(user.uid);

      if (!userData) {
        throw new Error('User data not found');
      }

      const connectionIds = (userData.connections ?? []) as string[];

      if (connectionIds.length === 0) {
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
      const connectedUsers = await getUsersByUIDs(connectionIds);

      // map skills to their names
      return connectedUsers.map((connectedUser) => {
        // map teach skills
        const teachSkills = (connectedUser.teach || []).map((skillId) => skillsMap[skillId]?.name || 'Unknown Skill');

        // map learn skills
        const learnSkills = (connectedUser.learn || []).map((skillId) => skillsMap[skillId]?.name || 'Unknown Skill');

        return {
          ...connectedUser,
          teach: teachSkills,
          learn: learnSkills,
        };
      });
    },
    enabled: !!user?.uid,
  });
};

export default useConnections;