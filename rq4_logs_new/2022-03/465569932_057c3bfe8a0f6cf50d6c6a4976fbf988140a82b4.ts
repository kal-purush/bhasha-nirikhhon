const census = require('citysdk');

import config from './config';

export interface CensusApiQuery {
  vintage: string;
  geoHierarchy: CensusApiGeoHierarchy;
  sourcePath: string[];
  values: string[];
  statsKey?: string;
}

export interface CensusApiGeoHierarchy {
  state: string;
  county: string;
  'block-group': string;
}

export function runCensusTractDataQuery(
    state: string, counties: string[], values: string[], vintage='2020',
    blockGroup='*', sourcePath=['acs','acs5']): Promise<any> {

  const queryConfig = createQueryConfig(state, counties, values, vintage, blockGroup, sourcePath);

  return new Promise(resolve => census(queryConfig, (error: any, response: any) => resolve(response)));
}


function createQueryConfig(
    state: string, counties: string[], values: string[], vintage: string,
    blockGroup: string, sourcePath: string[]): CensusApiQuery {

  const queryConfig: CensusApiQuery = {
    vintage,
    geoHierarchy: {
      state,
      county: counties.join(','),
      'block-group': blockGroup
    },
    sourcePath,
    values
  };

  if (config && config.censusApiKey) {
    queryConfig.statsKey = config.censusApiKey;
  }

  return queryConfig;
}

function findTract(respone: Array<any>, tractId: string): any {
  for(const item of respone) {
    if (item['tract'] === tractId) {
      return item;
    }
  }

  throw Error(`Could not find tract ${tractId}`);
}