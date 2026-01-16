using Microsoft.AspNetCore.Mvc;
using Igtampe.Neco.Common;
using Igtampe.Neco.Common.Banking;
using Igtampe.Neco.Common.Income;
using Igtampe.Neco.Common.Taxes;
using Igtampe.Neco.Data;
using Igtampe.ChopoSessionManager;
using Microsoft.EntityFrameworkCore;

namespace Igtampe.Neco.API.Controllers {

    /// <summary>Controller that handles User operations</summary>
    [Route("API/Statistics")]
    [ApiController]
    public class StatisticsController : ControllerBase {

        private readonly NecoContext DB;

        /// <summary>Creates a User Controller</summary>
        /// <param name="Context"></param>
        public StatisticsController(NecoContext Context) => DB = Context;

        /// <summary>Gets a month by month breakdown of transaction amount and count month by month</summary>
        /// <param name="StartDate"></param>
        /// <param name="EndDate"></param>
        /// <returns></returns>
        //Total transactions month by month
        //Total transaction amount month by month
        [HttpGet("Transactions/Monthly")]
        public async Task<IActionResult> TransactionsMonthly([FromQuery] DateTime? StartDate, [FromQuery] DateTime? EndDate) {

            DateTime Start = StartDate ?? DateTime.Now.AddMonths(-6).AddDays(-DateTime.Now.Day);
            DateTime End = EndDate ?? DateTime.Now;

            //D a m n 
            var Data = await DB.Transaction.Where(T=>T.Date > Start && T.Date < End).GroupBy(T => new { T.Date.Month, T.Date.Year })
                .Select(T => new { T.Key.Year, T.Key.Month, Count = T.Count(), Sum = T.Sum(T => T.Amount) }).ToListAsync();
                
            return Ok(Data);

        }

        /// <summary>Gets global overall statistics on transactions</summary>
        /// <returns></returns>
        //Total transaction to date
        //Total transaction amount to date
        [HttpGet("Transactions")]
        public async Task<IActionResult> Transactions() {

            long Sum = await DB.Transaction.SumAsync(T => T.Amount);
            long Count = await DB.Transaction.CountAsync();
            return Ok(new { Sum, Count });
        
        }

        /// <summary>Runs a Model Tax day to see how much each jurisdiction is projected to make</summary>
        /// <returns></returns>
        //Total amount taken by tax (involves generating tax reports)
        [HttpGet("TaxDayReport")]
        public async Task<IActionResult> JurisdictionsReport() {

            //We only really need the IDs of the accounts
            List<string?> Accounts = await DB.Account.Select(A => A.ID).ToListAsync();
            Dictionary<Jurisdiction, long> PaymentDictionary = new();

            foreach (string? ID in Accounts) {

                if (ID is null) { continue; }

                //Generate a tax report for this account
                TaxReport TR = await TaxController.GenerateReport(DB, ID);
                if (TR.IsEmpty || TR.Account is null) { continue; }

                //Add the Jurisdictions to the payment dictionary
                foreach (Jurisdiction A in TR.TaxPaymentDictionary.Keys) {
                    if (PaymentDictionary.ContainsKey(A)) { PaymentDictionary[A] += TR.TaxPaymentDictionary[A]; } 
                    else { PaymentDictionary.Add(A, TR.TaxPaymentDictionary[A]); }
                }
            }

            return Ok(PaymentDictionary);

        }

        //Total amount of each type of income entities
        //Total amount of each type of static monthly income generated.

        /// <summary>Gets statistics for all airlines in the Neco system</summary>
        /// <returns></returns>
        [HttpGet("Income/Airlines")]
        public async Task<IActionResult> AirlineStatistics() => await IncomeItemStatistics(DB.Airline);
        
        /// <summary>Gets statistics for all apartments in the Neco system</summary>
        /// <returns></returns>
        [HttpGet("Income/Apartments")]
        public async Task<IActionResult> ApartmentStatistics() => await IncomeItemStatistics(DB.Apartment);
        
        /// <summary>Gets statistics for all businesses in the Neco system</summary>
        /// <returns></returns>
        [HttpGet("Income/Businesses")]
        public async Task<IActionResult> BusinessStatistics() => await IncomeItemStatistics(DB.Business);
        
        /// <summary>Gets statistics for all corporations in the Neco system</summary>
        /// <returns></returns>
        [HttpGet("Income/Corporations")]
        public async Task<IActionResult> CorporationStatistics() => await IncomeItemStatistics(DB.Corporation);
        
        /// <summary>Gets statistics for all hotels in the neco system</summary>
        /// <returns></returns>
        [HttpGet("Income/Hotel")]
        public async Task<IActionResult> HotelStatistics() => await IncomeItemStatistics(DB.Hotel);

        private async Task<IActionResult> IncomeItemStatistics<E>(IQueryable<E> BaseSet) where E : IncomeItem {

#pragma warning disable CS8602 // Dereference of a possibly null reference. //Wheres still don't deal with dereference
            var Breakdown = await BaseSet.Where(T => T.Jurisdiction != null).GroupBy(T => new { T.Jurisdiction.ID, T.Jurisdiction.Name })
                .Select(T => new { T.Key.ID, T.Key.Name, Count = T.Count(), Income = T.Sum(T => T.Income()) }).ToListAsync();
#pragma warning restore CS8602 // Dereference of a possibly null reference.

            int TotalCount = Breakdown.Sum(T => T.Count);
            long TotalIncome = Breakdown.Sum(T => T.Income);

            return Ok(new { Breakdown, TotalCount, TotalIncome });
        
        }

        //Total amount of monthly income generated
        /// <summary>Gets total income statistics for all types of income items in the neco system</summary>
        /// <typeparam name="E"></typeparam>
        /// <returns></returns>
        [HttpGet("Income")]
        public async Task<IActionResult> Income<E>() {

#pragma warning disable CS8602 // Dereference of a possibly null reference. //Wheres still don't deal with dereference
            var TheMegaQuery = await
                DB.Airline.Where(T => T.Jurisdiction != null).GroupBy(T => new { T.Jurisdiction.ID, T.Jurisdiction.Name })
                .Select(T => new { T.Key.ID, T.Key.Name, Count = T.Count(), Income = T.Sum(T => T.Income()) }).Union(
                    DB.Apartment.Where(T => T.Jurisdiction != null).GroupBy(T => new { T.Jurisdiction.ID, T.Jurisdiction.Name })
                .Select(T => new { T.Key.ID, T.Key.Name, Count = T.Count(), Income = T.Sum(T => T.Income()) })).Union(
                    DB.Business.Where(T => T.Jurisdiction != null).GroupBy(T => new { T.Jurisdiction.ID, T.Jurisdiction.Name }) //Good god what the *heck*
                .Select(T => new { T.Key.ID, T.Key.Name, Count = T.Count(), Income = T.Sum(T => T.Income()) })).Union(
                    DB.Corporation.Where(T => T.Jurisdiction != null).GroupBy(T => new { T.Jurisdiction.ID, T.Jurisdiction.Name })
                .Select(T => new { T.Key.ID, T.Key.Name, Count = T.Count(), Income = T.Sum(T => T.Income()) })).Union(
                    DB.Hotel.Where(T => T.Jurisdiction != null).GroupBy(T => new { T.Jurisdiction.ID, T.Jurisdiction.Name })
                .Select(T => new { T.Key.ID, T.Key.Name, Count = T.Count(), Income = T.Sum(T => T.Income()) }))
                .GroupBy(T => new { T.ID, T.Name}).Select(T => new { T.Key.ID, T.Key.Name, Count = T.Sum(T=>T.Count), Income = T.Sum(T => T.Income) })
                .ToListAsync(); //REGROUP AND RE-SELECT

            //What on earth did we create.

#pragma warning restore CS8602 // Dereference of a possibly null reference.

            int TotalCount = TheMegaQuery.Sum(T => T.Count);
            long TotalIncome = TheMegaQuery.Sum(T => T.Income);

            return Ok(new { Breakdown = TheMegaQuery, TotalCount, TotalIncome });

        }

        /// <summary>Gets statistics for all banks in the Neco system</summary>
        /// <returns></returns>
        [HttpGet("Banks")]
        public async Task<IActionResult> Banks() {

            //Get all Banks, include accounts
#pragma warning disable CS8602 // Dereference of a possibly null reference. //Wheres still don't deal with dereference
            var AccountSummary = await DB.Account.Where(T => T.Bank != null)
                .GroupBy(T => new { T.Bank.ID, T.Bank.Name, T.Bank.ImageURL }) //We don't calculate market share even though with SQL we probably could (:Shrug:)
                .Select(T => new { T.Key.ID, T.Key.Name, T.Key.ImageURL, Count = T.Count(), Balance = T.Sum(T => T.Balance) }).ToListAsync();
#pragma warning restore CS8602 // Dereference of a possibly null reference.

            int TotalCount = AccountSummary.Sum(T => T.Count);
            long TotalMoney = AccountSummary.Sum(T => T.Balance);

            return Ok(new {AccountSummary, TotalCount, TotalMoney});

        }
    }
}