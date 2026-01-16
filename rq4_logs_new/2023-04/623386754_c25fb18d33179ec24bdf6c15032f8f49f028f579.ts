interface CurrencyUpperCaseInterface {
  currency: "USD" | "GBP" | "PLN" | "EUR";
}

interface CurrencyExchangeInterface {
  from: CurrencyUpperCaseInterface["currency"];
  to: CurrencyUpperCaseInterface["currency"];
  amount: number;
}

export default CurrencyExchangeInterface;