using System;
using Tribal.Backend.CreditLine.Domain;
using Tribal.Backend.CreditLine.Domain.Enums;
using Tribal.Backend.CreditLine.Domain.Models;

namespace Tribal.Backend.CreditLine.Application
{
    public class CreditLineRequestService
    {
        private const int RATIO_CASHBALANCE = 3;
        private const int RATIO_MONTHLYREVENUE = 5;

        public CreditLineResponseModel DetermineCreditLine(CreditLineRequestModel request)
        {
            CreditLineResponseModel response = null;
            decimal authorizedCreditLine;
            bool isAccepted;
            try
            {
                authorizedCreditLine = CalculateCreditLine(request.MonthlyRevenue, request.CashBalance, request.FoundingType);
                isAccepted = AcceptCreditLine(request.RequestedCreditLine, authorizedCreditLine);

                response = new();
                response.AuthorizedCreditLine = authorizedCreditLine;
                response.IsAccepted = isAccepted;
            }
            catch (ArgumentNullException ex)
            {
                response = null;
            }
            catch (Exception ex)
            {
                throw new ApplicationException("Error in DetermineCreditLine method.", ex);
            }

            return response;
        }

        public decimal CalculateCreditLine(decimal monthlyRevenue, decimal cashBalance, string foundingType)
        {
            try
            {
                decimal creditLineByMonthlyRevenue = monthlyRevenue / RATIO_MONTHLYREVENUE;
                decimal creditLineByCashBalance = cashBalance / RATIO_CASHBALANCE;
                bool checkFoundingTypeSME;

                if (String.IsNullOrWhiteSpace(foundingType))
                    throw new ArgumentNullException(nameof(foundingType));
                checkFoundingTypeSME = foundingType == FoundingTypeEnum.SME.ToString();

                decimal recommendedCreditLine;
                if (checkFoundingTypeSME)
                {
                    recommendedCreditLine = creditLineByMonthlyRevenue;
                }
                else
                {
                    recommendedCreditLine = Math.Max(creditLineByMonthlyRevenue, creditLineByCashBalance);
                }

                return recommendedCreditLine;

            }
            catch (ArgumentNullException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw new ApplicationException("Error in CalculateCreditLine", ex);
            }
        }

        public bool AcceptCreditLine(decimal requestedCreditLine, decimal recommendedCreditLine)
        {
            bool passedCreditLine = false;

            passedCreditLine = recommendedCreditLine > requestedCreditLine;

            return passedCreditLine;
        }
    }
}