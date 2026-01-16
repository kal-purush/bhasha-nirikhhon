import { CurrenciesName } from "../types/constants";

const API_URL = 'https://currate.ru/api/?get=rates&pairs=RUBUSD,RUBEUR&key=9199e7a568b3f1b9661dffa40e2520d5';

interface Response {
  data: Data;
  message: string;
}

interface Data {
  [CurrenciesName.USD]: number;
  [CurrenciesName.EUR]: number;
}

export const getRates = async (): Promise<Data> => {
  const response = await fetch(API_URL);
  const { data } = await response.json();

  return {
    [CurrenciesName.USD]: Number(data.RUBEUR),
    [CurrenciesName.EUR]: Number(data.RUBUSD),
  };
};