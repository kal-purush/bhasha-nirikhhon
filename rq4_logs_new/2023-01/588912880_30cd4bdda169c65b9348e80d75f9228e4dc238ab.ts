export type Repository = {
  id: string;
  name: string;
  mainLanguage: string;
  lastUpdate: string;
  commitCount: number;
  languages: {
    name: string;
    rate: number;
  }[];
};

const repositories: Repository[] = [
  {
    id: '0',
    name: '2022_wd2a',
    mainLanguage: 'Swift',
    lastUpdate: 'Oct 29',
    commitCount: 98,
    languages: [
      {
        name: 'Swift',
        rate: 85,
      },
      {
        name: 'Go',
        rate: 74,
      },
      {
        name: 'Ruby',
        rate: 61,
      },
      {
        name: 'PHP',
        rate: 60,
      },
    ],
  },
  {
    id: '1',
    name: 'gitfish-mobile',
    mainLanguage: 'Python',
    lastUpdate: 'Oct 27',
    commitCount: 34,
    languages: [
      {
        name: 'Python',
        rate: 83,
      },
      {
        name: 'Go',
        rate: 77,
      },
      {
        name: 'Ruby',
        rate: 68,
      },
      {
        name: 'PHP',
        rate: 44,
      },
    ],
  },
  {
    id: '2',
    name: 'gitfish-web',
    mainLanguage: 'JavaScript',
    lastUpdate: 'Oct 27',
    commitCount: 12,
    languages: [
      {
        name: 'JavaScript',
        rate: 98,
      },
      {
        name: 'Go',
        rate: 78,
      },
      {
        name: 'Ruby',
        rate: 57,
      },
      {
        name: 'PHP',
        rate: 33,
      },
    ],
  },
  {
    id: '3',
    name: '2023_wd3a',
    mainLanguage: 'TypeScript',
    lastUpdate: 'Oct 27',
    commitCount: 59,
    languages: [
      {
        name: 'TypeScript',
        rate: 97,
      },
      {
        name: 'Go',
        rate: 74,
      },
      {
        name: 'Ruby',
        rate: 68,
      },
      {
        name: 'PHP',
        rate: 56,
      },
    ],
  },
  {
    id: '4',
    name: 'liff-starter',
    mainLanguage: 'Dart',
    lastUpdate: 'Oct 27',
    commitCount: 73,
    languages: [
      {
        name: 'Dart',
        rate: 94,
      },
      {
        name: 'Go',
        rate: 71,
      },
      {
        name: 'Ruby',
        rate: 54,
      },
      {
        name: 'PHP',
        rate: 42,
      },
    ],
  },
  {
    id: '5',
    name: 'bat-fish',
    mainLanguage: 'Rust',
    lastUpdate: 'Oct 27',
    commitCount: 70,
    languages: [
      {
        name: 'Rust',
        rate: 86,
      },
      {
        name: 'Go',
        rate: 71,
      },
      {
        name: 'Ruby',
        rate: 61,
      },
      {
        name: 'PHP',
        rate: 34,
      },
    ],
  },
];

export default repositories;