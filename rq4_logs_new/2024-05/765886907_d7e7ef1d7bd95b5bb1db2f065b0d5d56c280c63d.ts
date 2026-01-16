const data = [
  {
    id: 1,
    url: 'https://credit.niso.org/contributor-roles/project-administration/',
    label: 'Principal Investigator (PI)',
    description: 'An individual conducting a research and investigation process, specifically performing the experiments, or data/evidence collection.',
    displayOrder: 1,
  },
  {
    id: 2,
    url: 'https://credit.niso.org/contributor-roles/project-administration/',
    label: 'Project Administrator',
    description: 'An individual with management and coordination responsibility for the research activity planning and execution.',
    displayOrder: 2,
  },
  {
    id: 3,
    url: 'https://credit.niso.org/contributor-roles/data-curation/',
    label: 'Data Manager',
    description: 'An individual engaged in management activities to annotate (produce metadata), scrub data and maintain research data (including software code, where it is necessary for interpreting the data itself) for initial use and later re-use.',
    displayOrder: 3,
  },
  {
    id: 4,
    url: `${process.env.DMSP_BASE_URL}/contributor_roles/other`,
    label: 'Other',
    description: 'An individual associated with the research project that does not fall under one of the other primary roles.',
    displayOrder: 4,
  },
]

export const mock = {
  // Return a random item from the data array
  ContributorRole: () => data[Math.floor(Math.random() * data.length)]
}