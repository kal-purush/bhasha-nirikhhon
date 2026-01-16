import { PreviewCardProps } from '@/components/common/preview-card';

export const Categories = [
  { id: 1, name: 'Code' },
  { id: 2, name: 'Design' },
  { id: 3, name: 'Data Science' },
];

export const Profiles: Omit<PreviewCardProps, 'button'>[] = [
  {
    id: 1,
    name: 'Mike Johnson',
    percent: 100,
    teach: ['Python', 'C++', 'Java', 'React', 'Node.js'],
    learn: ['Javascript'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
  {
    id: 2,
    name: 'Mike Johnson',
    percent: 100,
    teach: ['Python', 'C++'],
    learn: ['Javascript'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
  {
    id: 3,
    name: 'Hailey',
    percent: 100,
    teach: ['Python', 'C++'],
    learn: ['Javascript'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
  {
    id: 4,
    name: 'Alice',
    percent: 100,
    teach: ['Python', 'C++'],
    learn: ['Javascript', 'Java', 'React', 'Node.js'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
  {
    id: 5,
    name: 'Jane',
    percent: 100,
    teach: ['Python', 'C++'],
    learn: ['Javascript'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
  {
    id: 6,
    name: 'John',
    percent: 100,
    teach: ['Python', 'C++', 'Java', 'React', 'Node.js'],
    learn: ['Javascript'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
  {
    id: 7,
    name: 'Mike Johnson',
    percent: 100,
    teach: ['Python', 'C++'],
    learn: ['Javascript'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
  {
    id: 8,
    name: 'Mike Johnson',
    percent: 100,
    teach: ['Python', 'C++'],
    learn: ['Javascript'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
  {
    id: 9,
    name: 'Mike Johnson',
    percent: 100,
    teach: ['Python', 'C++'],
    learn: ['Javascript'],
    photoUrl: '/placeholder.svg?height=100&width=100',
  },
];