export interface LoanProduct{
    uniqueId: string;
  cappingPercentage: number;
  cappingMethod: string;
  interestType: string;
  name: string;
  description: string;
  defaultLoanAmount: number;
  minLoanAmount: number;
  maxLoanAmount: number;
  defaultNumInstallments: number;
  minNumInstallments: number;
  maxNumInstallments: number;
  defaultGracePeriod: number;
  minGracePeriod: number;
  maxGracePeriod: number;
  gracePeriodType: string;
  defaultRepaymentPeriodCount: number;
  repaymentPeriodUnit: string;
  defaultPenaltyRate: number;
  minPenaltyRate: number;
  maxPenaltyRate: number;
  interestCalculationMethod: string;
  loanPenaltyCalculationMethod: string;
  valid: boolean;
}