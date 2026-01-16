import { isPartOf } from './common-typeguard';

export const areEarnings = (testedObject: any): boolean => {
  const earningsProperties = ['symbol', 'earnings'];
  const firstEarning = testedObject.earnings[0];
  return isEarning(firstEarning) && isPartOf(earningsProperties, testedObject);
};

export const isEarning = (testedObject: any): boolean => {
  const earningProperties = [
    'actualEPS',
    'consensusEPS',
    'announceTime',
    'numberOfEstimates',
    'EPSSurpriseDollar',
    'EPSReportDate',
    'fiscalPeriod',
    'fiscalEndDate',
    'yearAgo',
    'yearAgoChangePercent',
  ];

  return isPartOf(earningProperties, testedObject);
};