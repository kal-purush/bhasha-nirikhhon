import { Skill } from '@/types/skill.type';
import { User } from '@/types/user.type';

export const allskills: Skill[] = [
  { id: 'javascript', name: 'JavaScript', category: 'frontend' },
  { id: 'typescript', name: 'TypeScript', category: 'frontend' },
  { id: 'react', name: 'React', category: 'frontend' },
  { id: 'vue', name: 'Vue', category: 'frontend' },
  { id: 'angular', name: 'Angular', category: 'frontend' },
  { id: 'nodejs', name: 'NodeJS', category: 'backend' },
  { id: 'express', name: 'Express', category: 'backend' },
  { id: 'python', name: 'Python', category: 'backend' },
  { id: 'django', name: 'Django', category: 'backend' },
  { id: 'flask', name: 'Flask', category: 'backend' },
  { id: 'mongodb', name: 'MongoDB', category: 'database' },
  { id: 'mysql', name: 'MySQL', category: 'database' },
  { id: 'postgresql', name: 'PostgreSQL', category: 'database' },
];

export const fakeUser: User = {
    id: '1',
    email: 'example@gmail.com',
    fullName: 'Joe',
    bio: 'Hello',
    connections: [],
    learn: ['typescript', 'mongodb'],
    teach: ['angular'],
    requestConnections: [],
    sentConnections: [],
    photoURL: '',
  };