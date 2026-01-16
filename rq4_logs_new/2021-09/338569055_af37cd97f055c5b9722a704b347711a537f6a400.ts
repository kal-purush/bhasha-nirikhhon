import Project from 'types/project';

export const loyalty: Project = {
  description:
    'I created the initial loyalty system for Polaris Elements solo, although it has been worked on by others since then. It is written in PHP using the CodeIgniter framework. There is a Bootstrap UI and a REST API for the backend connecting to a MySQL database.',
  name: 'Polaris Loyalty',
  url: 'https://polaris-elements.co.uk/solutions/polaris-infinity/'
};
export const thisWebsite: Project = {
  description:
    'Personal website created with Next.js. Written in TypesScript with TailwindCSS. Uses Jest to achieve 100% test coverage. Automatic type checking and linting with Husky, Prettier and ESLint. GitHub actions to automatically apply dependency updates and to ensure all tests pass before merging a PR.',
  image: '/peteoshea.co.uk.png',
  name: 'Source for this website',
  url: 'https://github.com/peteoshea/website/'
};
export const saransurf: Project = {
  description:
    'Simple E-commerce website created from scratch with Next.js. Makes use of Crystallize GraphQL API to place orders and update inventory.',
  image: '/saransurf.co.uk.png',
  name: 'Saransurf',
  url: 'https://saransurf.co.uk/'
};
export const skipad: Project = {
  description: 'iPhone app written in Objective C. Unfortunately no longer available.',
  image: '/SkiPad_iPhone_4_Detail.png',
  name: 'SkiPad iPhone App',
  url: 'https://youtu.be/BnXpRpjBIzE'
};

const myPortfolio: Project[] = [thisWebsite, saransurf, skipad, loyalty];

export default myPortfolio;