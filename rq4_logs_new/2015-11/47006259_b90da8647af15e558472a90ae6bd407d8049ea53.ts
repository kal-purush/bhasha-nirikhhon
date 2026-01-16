export const techs: Sco.Model.TechAsset[] = [
  {
    id: 'tech_galaxyTrade',
    name: 'Galaxy trade',
    cost: { GL: 500 },
    productionTime: 2,
    description: 'Allows to build Market Places',
    level: 1,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_iron1',
    name: 'Iron 1',
    cost: { GL: 300 },
    productionTime: 1,
    description: 'Allows to build Iron Mines',
    level: 1,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_gas1',
    name: 'Gas 1',
    cost: { GL: 300 },
    productionTime: 1,
    description: 'Allows to build Pipelines',
    level: 1,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_antiSpy',
    name: 'Anti-Spy tech',
    cost: { GL: 400 },
    productionTime: 2,
    description: 'Allows to build detection towers',
    level: 2,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_centralization',
    name: 'Centralization',
    cost: { GL: 700 },
    productionTime: 2,
    description: 'Allows to bluild the Palace',
    level: 2,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_advancedFarming',
    name: 'Advanced farming',
    cost: { GL: 1000 },
    productionTime: 2,
    description: 'Doubles farm benefits on Green planets',
    level: 3,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_advanedScience',
    name: 'Advaned science',
    cost: { GL: 500 },
    productionTime: 2,
    description: 'Allows players to research two technologies at once',
    level: 3,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_iron2',
    name: 'Iron 2',
    cost: { GL: 1500 },
    productionTime: 3,
    description: 'Allows to build Foundries on a Red Planet',
    level: 4,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_gas2',
    name: 'Gas 2',
    cost: { GL: 1500 },
    productionTime: 3,
    description: 'Allows to build Refinery on a Blue Planet',
    level: 4,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_mo1',
    name: 'MO 1',
    cost: { GL: 1500 },
    productionTime: 3,
    description: 'Allows to build MO quarries on a Dark planet',
    level: 4,
    family: 'techFamily_civil',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_smallShips',
    name: 'Small Ships',
    cost: { GL: 300 },
    productionTime: 2,
    description: 'Allows to produce Small Ships',
    level: 1,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_antiArmorLasers',
    name: 'Anti-Armor Lasers',
    cost: { GL: 400 },
    productionTime: 1,
    description: 'Allows to produce Ships with Anti-Armor Lasers',
    level: 1,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_cannons1',
    name: 'Cannons 1',
    cost: { GL: 300 },
    productionTime: 1,
    description: 'Allows to Build B6 cannons',
    level: 1,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_mediumShips',
    name: 'Medium Ships',
    cost: { GL: 600 },
    productionTime: 2,
    description: 'Allows to produce Medium Ships',
    level: 2,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_bombs',
    name: 'Bombs',
    cost: { GL: 500 },
    productionTime: 2,
    description: 'Allows to produce Ships with Bombs',
    level: 2,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_cannons2',
    name: 'Cannons 2',
    cost: { GL: 450 },
    productionTime: 1,
    description: 'Allows to build B12 and B18 cannons',
    level: 2,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_largeShips',
    name: 'Large Ships',
    cost: { GL: 1500 },
    productionTime: 3,
    description: 'Allows to produce Large Ships',
    level: 3,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_systemControl',
    name: 'System Control',
    cost: { GL: 750 },
    productionTime: 2,
    description: 'Allows to Build Radar Towers',
    level: 3,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_hugeShips',
    name: 'Huge Ships',
    cost: { GL: 2000 },
    productionTime: 4,
    description: 'Allows to produce Huge Shups',
    level: 4,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  },
  {
    id: 'tech_warp',
    name: 'Warp',
    cost: { GL: 2500 },
    productionTime: 3,
    description: 'Allows to build Warp Beacons and Warp Ships',
    level: 4,
    family: 'techFamily_military',
    techRequirements: [],
    features: []
  }
];

export const techFamilies: Sco.Model.TechFamilyAsset[] = [
  {
    id: 'techFamily_civil',
    name: 'Civil',
    color: '#D9D2E9',
    description: '',
  },
  {
    id: 'techFamily_military',
    name: 'Military',
    color: '#EA9999',
    description: '',
  }
];