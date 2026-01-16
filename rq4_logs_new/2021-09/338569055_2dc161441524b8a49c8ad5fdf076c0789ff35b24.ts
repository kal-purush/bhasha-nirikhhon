import Job from 'types/job';

export const brithene: Job = {
  startDate: '1997',
  endDate: '1998',
  title: 'Analyst Programmer',
  employer: 'Brithene Films',
  location: 'Bridgwater',
  description:
    'Database programmer for a manufacturing company. Developing mainly with UniVerse BASIC, but some contact with Visual Basic. The work comprised of controlling the entry, storage and analysis of data relating to production, sales and accounts along with maintaining a mainframe system.'
};
export const fbh: Job = {
  startDate: '2000',
  endDate: '2010',
  title: 'Consultant',
  employer: 'FBH Associates',
  location: 'Taunton',
  description:
    'Started off working on UniVerse, and later jBASE, systems for the first few years. This included system administration of Unix, and later various Linux, systems. Cross trained to Delphi, outside office hours, creating a small Windows application which was used internally for many years. Soon became an integral part of the Delphi team working on the main POS products. Roles included:\n' +
    '- Delphi development as part of a small team.\n' +
    '- Source control using CVS and later Subversion.\n' +
    '- Craft SQL to access the Firebird databases used to store the POS data.\n' +
    '- Design and create various reports for end users.\n' +
    '- Liaising with clients and partners to define development requirements.\n' +
    '- Creating specification documents and time estimates.'
};
export const freelance: Job = {
  startDate: '2010',
  endDate: 'Present',
  title: 'Freelance Software Developer',
  employer: 'Self Employed',
  location: 'Home',
  description:
    'Started a small business. Very varied work including:\n' +
    '- Creation of an iOS app to help find suitable ski accommodation.\n' +
    '- Bespoke Delphi application for a waste management company.\n' +
    '- Web site design and setup of e-commerce sites.\n' +
    '- Regular Delphi development for previous employer.\n' +
    'This was the sole source of income for two years but was scaled back after returning to full-time employment. Now just occasional ongoing maintenance and tinkering on personal projects.'
};
export const ibm: Job = {
  startDate: '1999',
  endDate: '1999',
  title: 'C/C++ Programmer',
  employer: 'IBM Global Services Australia',
  location: 'Sydney',
  description:
    '3 month contract during a year out travelling the world. Working as part of a team to check code for any Y2K issues. This expanded to checking a variety of different languages including Visual Basic, Java and Unix shell scripts along with C and C++. Some simple Java programming helped locate tokens and automate fixes.'
};
export const icl: Job = {
  startDate: '1994',
  endDate: '1995',
  title: 'Industrial Trainee',
  employer: 'ICL (International Computers Limited)',
  location: 'Bracknell',
  description:
    'Placement year while studying at university. Roles included the following:\n' +
    '- Programming in C under various Unix based systems.\n' +
    '- Writing shell scripts.\n' +
    '- Building, installing and testing new products on various machines.\n' +
    '- Preparation and execution of presentations for new products.'
};
export const polarisLeadDeveloper: Job = {
  startDate: '2013',
  endDate: 'Present',
  title: 'Lead Developer',
  employer: 'Polaris Elements',
  location: 'Taunton',
  description:
    'Quickly promoted to Lead Developer, managing a team of 5 developers working on multiple projects including Windows applications and services based around POS systems, written in Delphi, web applications, created with PHP, and legacy jBASE database systems. Tasks include:\n' +
    '- Actively developing on all of the Delphi and PHP projects.\n' +
    '- Allocation of work to ensure the various projects stayed on track.\n' +
    '- Mentoring developers to help improve the quality of code produced.\n' +
    '- Unit testing, including getting a lot of legacy Delphi code under test.\n' +
    'After a few years, due to the implementation of Scrum and some company restructuring, I stopped managing a team but kept the title as leading by example and mentoring were still part of the role:\n' +
    '- Mainly working on the flagship POS product writing Delphi code using TDD.\n' +
    '- Create and maintain infrastructure as code using Terraform and Ansible for various projects.\n' +
    '- Occasional work on other projects using both PHP & JavaScript, specifically React.\n' +
    '- Performed the role of Scrum Master during our initial switch over to an agile approach.'
};
export const polarisSeniorDeveloper: Job = {
  startDate: '2012',
  endDate: '2013',
  title: 'Senior Developer',
  employer: 'Polaris Elements',
  location: 'Taunton',
  description:
    'Re-joined the Delphi team for previous employer after change of ownership and company name. Other tasks:\n' +
    '- Solo project to design and build online loyalty system with a RESTful web service API.\n' +
    '- Chose to write this in PHP using the CodeIgniter framework with a Bootstrap based front-end.\n' +
    '- Implemented a move of all source code from Subversion to Git which greatly improved development flexibility.\n' +
    '- Setting up and maintaining various Linux VMs including Apache and MySQL servers, a company wide mail server and a GitLab server all running on Citrix XenServer hosted on multiple dedicated servers.'
};

const myEmployment: Job[] = [
  polarisLeadDeveloper,
  polarisSeniorDeveloper,
  freelance,
  fbh,
  ibm,
  brithene,
  icl
];

export default myEmployment;