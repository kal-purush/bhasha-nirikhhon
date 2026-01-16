import { _get } from "../utils/ApiRequest";

export const ratesService = {
  getRate,
};

function getRate(source: string, dest: string) {
  return _get<string>(`rates?source=${source}&dest=${dest}`);
}