using System.Collections.Generic;
using FamilyBudget.Api.ViewModels;
using FamilyBudget.Domain.Entities;

namespace FamilyBudget.Api.Mappers
{
    public static class FinancialPeriodMapper
    {
        public static FinancialPeriodDto ToDto(this FinancialPeriod financialPeriod)
        {
            var result = new FinancialPeriodDto();
            if(financialPeriod == null)
                return result;

            result.Id = financialPeriod.Id;
            result.Name = financialPeriod.Name;
            result.PeriodBegin = financialPeriod.PeriodBegin;
            result.PeriodEnd = financialPeriod.PeriodEnd;
            result.Income = financialPeriod.Income;
            result.Expenditures = financialPeriod.Expenditures.MapExpenditures();

            return result;
        }

        private static List<ExpenditureDto> MapExpenditures(this List<Expenditure> expenditures)
        {
            var result = new List<ExpenditureDto>();
            if(expenditures == null)
                return result;
            
            expenditures.ForEach(e =>
            {
                result.Add(new ExpenditureDto
                {
                    Id = e.Id,
                    Name = e.Name,
                    Value = e.Value,
                    FinancialPeriodId = e.FinancialPeriodId,
                    PlannedExpenditureId = e.PlannedExpenditureId
                });
            });

            return result;
        }
    }
}