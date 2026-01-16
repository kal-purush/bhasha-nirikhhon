import currencyConverterClient from "./httpService";

export function getAllCurrencies(){
    return currencyConverterClient.get("/listquotes");
}

export function getConvertedCurrency(from, to){
    return currencyConverterClient.get(`/exchange?from=${from}&to=${to}`);
}