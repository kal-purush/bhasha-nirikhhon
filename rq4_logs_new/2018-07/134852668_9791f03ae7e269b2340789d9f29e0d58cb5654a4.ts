export interface ICompany {
  name: string;
  founder: string;
  founded: string;
  employees: number;
  vehicles: number;
  launch_sites: number;
  test_sites: number;
  ceo: string;
  cto: string;
  coo: string;
  cto_propulsion: string;
  valuation: string;
  headquarters: Iheadquarters;
  summary: string;
}

export interface Iheadquarters {
  address: string;
  city: string;
  state: string;
}