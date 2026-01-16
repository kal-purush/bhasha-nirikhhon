import CallApi from './services/service-call';
import { PRIVATE_SERVICE } from './services/configurations';

export const FetchNews = ({
  companySymbol,
  last = 5,
}: {
  companySymbol: string;
  last?: number;
}) => {
  return privateFetch({
    endpoint: `stock/${companySymbol}/news/last/${last}`,
  });
};

export const FetchKeyStats = ({
  companySymbol,
  stat = '',
  queryVariables,
}: {
  companySymbol: string;
  stat?: string;
  queryVariables?: string;
}) => {
  return privateFetch({
    endpoint: `stock/${companySymbol}/stats/${stat}`,
    queryVariables: queryVariables,
  });
};

export const FetchPeers = ({ companySymbol }: { companySymbol: string }) => {
  return privateFetch({
    endpoint: `stock/${companySymbol}/peers`,
  });
};

export const FetchOverview = ({ companySymbol }: { companySymbol: string }) => {
  return privateFetch({
    endpoint: `stock/${companySymbol}/company`,
  });
};

export const FetchQuote = ({
  companySymbol,
  queryVariables,
}: {
  companySymbol: string;
  queryVariables?: string;
}) => {
  return privateFetch({
    endpoint: `stock/${companySymbol}/quote`,
    queryVariables: queryVariables,
  });
};

export const FetchEarnings = ({
  companySymbol,
  timePeriod,
  queryVariables,
}: {
  companySymbol: string;
  timePeriod?: number;
  queryVariables?: string;
}) => {
  return privateFetch({
    endpoint: `stock/${companySymbol}/earnings/${timePeriod}`,
    queryVariables: queryVariables,
  });
};

const privateFetch = ({
  endpoint,
  queryVariables,
}: {
  endpoint: string;
  queryVariables?: string;
}) => {
  const fetchConfig = {
    endpoint: endpoint,
    config: PRIVATE_SERVICE,
    queryVariables: queryVariables,
  };

  return CallApi(fetchConfig);
};