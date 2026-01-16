// Esta função espera um objeto JS como argumento
// O objeto deve conter as seguintes propriedades
// - initialInvestment: O valor do investimento inicial
// - annualInvestment: O valor investido a cada ano
// - expectedReturn: A taxa de retorno esperada (anual)
// - duration: A duração do investimento (período de tempo)

type CalculateInvestmentResults = {
  initialInvestment: number;
  annualInvestment: number;
  expectedReturn: number;
  duration: number;
};

export function calculateInvestmentResults({
  initialInvestment,
  annualInvestment,
  expectedReturn,
  duration,
}: CalculateInvestmentResults) {
  const annualData = [];
  let investmentValue = initialInvestment;

  for (let i = 0; i < duration; i++) {
    const interestEarnedInYear = investmentValue * (expectedReturn / 100);
    investmentValue += interestEarnedInYear + annualInvestment;
    annualData.push({
      year: i + 1, // identificador do ano
      interest: interestEarnedInYear, // o valor dos juros ganhos neste ano
      valueEndOfYear: investmentValue, // valor do investimento no final do ano
      annualInvestment: annualInvestment, // investimento adicionado neste ano
    });
  }

  return annualData;
}

// A API Intl fornecida pelo navegador é usada para preparar um objeto formatador
// Este objeto oferece um método "format()" que pode ser usado para formatar números como moeda
// Exemplo de uso: formatter.format(1000) => yields "$1,000"
export const formatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
});