import Skill from 'types/skill';

export const agile: Skill = {
  description:
    'Agile development techniques, specifically Scrum, including some time as Scrum Master.'
};
export const ci: Skill = {
  description: 'CI (Continuous Integration) using GitHub Actions, Jenkins, Travis and CircleCI.'
};
export const delphi: Skill = {
  description:
    'Very experienced with Delphi, used as my main development environment for many years.'
};
export const desktop: Skill = {
  description: 'Current desktop OS of choice is Windows 11 but spent some time with MacOS.'
};
export const devops: Skill = {
  description:
    'DevOps experience setting up and maintaining various AWS resources with Terraform and Ansible.'
};
export const git: Skill = {
  description: 'Good working knowledge of Git using GitHub, BitBucket and GitLab.'
};
export const javascript: Skill = {
  description:
    'Javascript mainly using Node.js, React & Next.js with some brief exposure to Svelte & Vue.js.'
};
export const php: Skill = {
  description:
    'Full stack development using PHP including RESTful API design, implementation and use.'
};
export const python: Skill = {
  description: 'Use of Python to construct complex Fabric deployment scripts.'
};
export const scripting: Skill = {
  description: 'Automation using scripting languages like bash and PowerShell.'
};
export const sql: Skill = {
  description: 'Lots of practise writing SQL and designing MySQL and Firebird databases.'
};
export const sysadmin: Skill = {
  description: 'System administration skills with Linux (mainly Red Hat/Debian based systems).'
};
export const tdd: Skill = {
  description:
    'A love of TDD (Test Driven Development) which I feel has really improved my code quality.'
};
export const vagrant: Skill = {
  description:
    'Virtualisation of development environments using Vagrant. (Looking to make more use of Docker)'
};

const mySkills: Skill[] = [
  javascript,
  php,
  delphi,
  tdd,
  devops,
  ci,
  sysadmin,
  agile,
  git,
  sql,
  desktop,
  scripting,
  python,
  vagrant
];

export default mySkills;