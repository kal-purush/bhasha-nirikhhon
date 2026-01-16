using Microsoft.AspNetCore.Mvc;
using Igtampe.Neco.Common;
using Igtampe.Neco.Common.Banking;
using Igtampe.Neco.Common.Taxes;
using Igtampe.Neco.Common.Income;
using Igtampe.Neco.Data;
using Igtampe.Neco.API.Requests;
using Igtampe.ChopoSessionManager;
using Microsoft.EntityFrameworkCore;

namespace Igtampe.Neco.API.Controllers {

    /// <summary>Controller that handles User operations</summary>
    [Route("API/Taxes")]
    [ApiController]
    public class TaxController : ControllerBase {

        private readonly NecoContext DB;

        /// <summary>Creates a User Controller</summary>
        /// <param name="Context"></param>
        public TaxController(NecoContext Context) => DB = Context;

        /// <summary>Gets Jurisdictions based on a type</summary>
        /// <param name="Type">Type of jurisdictions to return. By default: Country</param>
        /// <param name="Query"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <returns></returns>
        [HttpGet("Jurisdictions")]
        public async Task<IActionResult> GetJurisdictions([FromQuery] JurisdictionType? Type, [FromQuery] string? Query, int? Skip, int? Take) {

            IQueryable<Jurisdiction> Set = DB.Jurisdiction.Where(J => J.Type == (Type ?? JurisdictionType.COUNTRY));
            if (!string.IsNullOrWhiteSpace(Query)) { Set = Set.Where(J => J.Name.ToLower().Contains(Query)); }

            Set = Set.Skip(Skip ?? 0).Take(Take ?? 20);

            return Ok(await Set.ToListAsync());

        }

        /// <summary>Gets a specific jurisdiction</summary>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("Jurisdictions/{ID}")]
        public async Task<IActionResult> GetJurisdiction(string ID) {

            Jurisdiction? J = await DB.Jurisdiction.Include(J => J.ParentJurisdiction).FirstOrDefaultAsync(J => J.ID == ID);
            return J is null ? NotFound(ErrorResult.NotFound("Jurisdiction was not found","ID")) : Ok(J);

        }

        /// <summary>Gets a specific jurisdiction's children</summary>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("Jurisdictions/{ID}/Children")]
        public async Task<IActionResult> GetJurisdictionChildren(string ID) {

            Jurisdiction? J = await DB.Jurisdiction.Include(J => J.ChildJurisdictions).Include(J=>J.ParentJurisdiction).FirstOrDefaultAsync(J => J.ID == ID);
            return J is null ? NotFound(ErrorResult.NotFound("Jurisdiction was not found", "ID")) : Ok(J.ChildJurisdictions);

        }

        /// <summary>Gets a specific Jurisdiction's brackets</summary>
        /// <returns></returns>
        [HttpGet("Jurisdiction/{ID}/Brackets")]
        public async Task<IActionResult> GetJurisdictionBrackets(string ID) {

            Jurisdiction? J = await DB.Jurisdiction.Include(J => J.Brackets).FirstOrDefaultAsync(J => J.ID == ID);
            return J is null ? NotFound(ErrorResult.NotFound("Jurisdiction was not found", "ID")) : Ok(J.ChildJurisdictions);

        }

        /// <summary>Gets a specific bracket</summary>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("Bracket/{ID}")]
        public async Task<IActionResult> GetBracket(Guid ID) {
            Bracket? B = await DB.Bracket.FindAsync(ID);
            return B is null ? NotFound(ErrorResult.NotFound("Bracket was not found","ID")): Ok(B);
        }

        /// <summary>Generates a tax report for given account ID</summary>
        /// <param name="SessionID"></param>
        /// <param name="AccountID"></param>
        /// <returns></returns>
        [HttpGet("GenerateReport")]
        public async Task<IActionResult> GetTaxReport([FromHeader] Guid? SessionID, [FromQuery] string AccountID) {

            //Check the session:
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            //Check the account
            bool IsOwned = await DB.Account.AnyAsync(A => A.ID == AccountID && A.Owners.Any(O => O.ID==S.UserID));
            return !IsOwned && ! await IsAdmin(S.UserID)
                ? NotFound(ErrorResult.NotFound("Account was not found or is not owned by the session owner","Account"))
                : Ok(await GenerateReport(DB,AccountID));
        }

        /// <summary>Gets a previously saved tax report with given ID</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("Reports")]
        public async Task<IActionResult> GetTaxReports([FromHeader] Guid? SessionID, [FromQuery] string ID) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            List<TaxReport> TRs = await DB.TaxReport.Where(A => A.Account != null && A.Account.ID==ID && A.Account.Owners.Any(O => O.ID == S.UserID))
                .OrderByDescending(A => A.DateGenerated).ToListAsync(); ;
            return Ok(TRs);
        }

        /// <summary>Gets a previously saved tax report with given ID</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("Reports/{ID}")]
        public async Task<IActionResult> GetTaxReport([FromHeader] Guid? SessionID, [FromQuery] Guid ID) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            TaxReport? TR = await DB.TaxReport.FirstOrDefaultAsync(A => A.ID == ID && A.Account != null && A.Account.Owners.Any(O => O.ID == S.UserID));
            return TR is null
                ? NotFound(ErrorResult.NotFound("Tax Report was not found, or is not from an account this session owner owns","ID"))
                : Ok(TR);
        }

        /// <summary>Creates a jurisdiction</summary>
        /// <param name="SessionID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPost("Jurisdiction")]
        public async Task<IActionResult> CreateJurisdiction([FromHeader] Guid? SessionID, [FromBody] JurisdictionRequest Request) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            if (!await IsAdmin(S.UserID)) { return Forbid(); }

            Jurisdiction? Parent = null;
            if (!string.IsNullOrWhiteSpace(Request.ParentJurisdictionID)) { 
                Parent = await DB.Jurisdiction.FirstOrDefaultAsync(J => J.ID == Request.ParentJurisdictionID);
                if (Parent is null) { return NotFound(ErrorResult.NotFound("Parent jurisdiction was not found","Head")); }
            }

            Account? A = await DB.Account.FindAsync(Request.TiedAccountID);
            if(A is null) { return NotFound(ErrorResult.NotFound("Tied account was not found","Account")); }

            if (Request.Type != JurisdictionType.COUNTRY && Parent is null) { return BadRequest(ErrorResult.BadRequest("No parent jurisdiction is set and this is not a Country","Head")); }
            if (Parent is not null && Parent.Type != Request.Type - 1) { return BadRequest(ErrorResult.BadRequest("Parent type is not one layer above this jurisdiction", "Type")); }

            Jurisdiction J = new() {
                ImageURL = Request.ImageURL, Name = Request.Name,
                Population = Request.Population, Type = Request.Type,
                ParentJurisdiction = Parent,
                TiedAccount = A
            };

            do { J.ID=J.IDGenerator.Generate(); } while (await DB.Jurisdiction.AnyAsync(j=>j.ID==J.ID));

            //OK we're ready
            DB.Add(J);
            await DB.SaveChangesAsync();
            
            return Ok(J);
        }

        /// <summary>Creates a Tax Bracket</summary>
        /// <param name="SessionID"></param>
        /// <param name="Jurisdiction"></param>
        /// <param name="Bracket"></param>
        /// <returns></returns>
        [HttpPost("Brackets")]
        public async Task<IActionResult> CreateBracket([FromHeader] Guid? SessionID, [FromQuery] Guid? Jurisdiction,  [FromBody] Bracket Bracket) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            if (!await IsAdmin(S.UserID)) { return Forbid(); }

            //Find the jurisdiction
            Jurisdiction? J = await DB.Jurisdiction.FindAsync(Jurisdiction ?? Guid.Empty);
            if (J is null) { return NotFound(ErrorResult.NotFound("Jurisdiction this bracket will belong to was not found","Jurisdiction")); }

            Bracket.Jurisdiction = J;

            DB.Add(Bracket);
            await DB.SaveChangesAsync();

            return Ok(Bracket);
        }

        /// <summary>Updates a Jurisdiction</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPut("Jurisdiction/{ID}")]
        public async Task<IActionResult> UpdateJurisdiction([FromHeader] Guid? SessionID, string ID, [FromBody] JurisdictionRequest Request) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            if (!await IsAdmin(S.UserID)) { return Forbid(); }

            Jurisdiction? J = await DB.Jurisdiction.Include(J => J.ChildJurisdictions).FirstOrDefaultAsync(J=>J.ID==ID);
            if(J is null) { return NotFound(ErrorResult.NotFound("Jurisdiction was not found","ID")); }

            Jurisdiction? Parent = null;
            if (!string.IsNullOrWhiteSpace(Request.ParentJurisdictionID)) {
                Parent = await DB.Jurisdiction.FirstOrDefaultAsync(J => J.ID == Request.ParentJurisdictionID);
                if (Parent is null) { return NotFound(ErrorResult.NotFound("Parent jurisdiction was not found", "Head")); }
            }

            Account? A = await DB.Account.FindAsync(Request.TiedAccountID);
            if (A is null) { return NotFound(ErrorResult.NotFound("Tied account was not found", "Account")); }

            //Change in type:
            //We could hypothetically update all of the children pero sabes? That's going to be far too much.
            if (J.Type != Request.Type && !J.ChildJurisdictions.Any()) { return BadRequest(ErrorResult.BadRequest("Cannot change jurisdiction type if this jurisdiction has children","Type")); }

            if (Request.Type != JurisdictionType.COUNTRY && Parent is null) { return BadRequest(ErrorResult.BadRequest("No parent jurisdiction is set and this is not a Country", "Head")); }
            if (Parent is not null && Parent.Type != Request.Type - 1) { return BadRequest(ErrorResult.BadRequest("Parent type is not one layer above this jurisdiction", "Type")); }

            J.Name = Request.Name;
            J.ParentJurisdiction = Parent;
            J.ImageURL = Request.ImageURL;
            J.Population = Request.Population;
            J.TiedAccount = A;
            J.Type = Request.Type;

            //OK we're ready
            DB.Update(J);
            await DB.SaveChangesAsync();

            return Ok(J);

        }

        /// <summary>Edits a bracket</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <param name="Bracket"></param>
        /// <returns></returns>
        [HttpPut("Bracket/{ID}")]
        public async Task<IActionResult> UpdateBracket([FromHeader] Guid? SessionID, string ID, Bracket Bracket) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            if (!await IsAdmin(S.UserID)) { return Forbid(); }

            //Find the existing bracket
            Bracket? B = await DB.Bracket.FindAsync(ID);
            if (B is null) { return NotFound(ErrorResult.NotFound("Bracket was not found","ID")); }
            
            B.Name = Bracket.Name;
            B.Description = Bracket.Description;
            B.Rate = Bracket.Rate;
            B.Start = Bracket.Start;
            B.End = Bracket.End;
            B.IncomeType = Bracket.IncomeType;

            DB.Update(B);
            await DB.SaveChangesAsync();

            return Ok(B);

        }

        /// <summary>Deletes a Bracket</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpDelete("Bracket/{ID}")]
        public async Task<IActionResult> DeleteBracket([FromHeader] Guid? SessionID, string ID) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid session"); }

            if (!await IsAdmin(S.UserID)) { return Forbid(); }

            Bracket? B = await DB.Bracket.FindAsync(ID);
            if (B is null) { return NotFound(ErrorResult.NotFound("Bracket was not found", "ID")); }

            DB.Remove(B);
            await DB.SaveChangesAsync();
            return Ok(B);

        }

        /// <summary>Executes a Tax Day event</summary>
        /// <param name="SessionID"></param>
        /// <param name="Force"></param>
        /// <returns></returns> 
        [HttpPost("TaxDay")] //There's no real reason to make this a post, but I just want this to not be accessible via a standard web-browser
        public async Task<IActionResult> TaxDay([FromHeader] Guid? SessionID, [FromQuery] bool? Force) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid Session"); }

            if (!await IsAdmin(S.UserID)) { return Forbid(); }

            if (DateTime.Now.Day != 1 && Force != true) { return BadRequest("It is not currently tax day! If you wish to run tax anyways, add Force=true"); }

            //We onyly really need the IDs of the accounts
            List<string?> Accounts = await DB.Account.Select(A => A.ID).ToListAsync();
            Dictionary<Account, long> PaymentDictionary = new();

            foreach (string? ID in Accounts) {

                if (ID is null) { continue; }

                //Generate a tax report for this account
                TaxReport TR = await GenerateReport(DB, ID);
                if (TR.IsEmpty || TR.Account is null) { continue; }

                //Add the Jurisdictions to the payment dictionary
                foreach (Jurisdiction A in TR.TaxPaymentDictionary.Keys) {

                    if (A.TiedAccount is null) { throw new ArgumentException("Jurisdiction tied accounts are not included"); }

                    if (PaymentDictionary.ContainsKey(A.TiedAccount)) { PaymentDictionary[A.TiedAccount] += TR.TaxPaymentDictionary[A]; } 
                    else { PaymentDictionary.Add(A.TiedAccount, TR.TaxPaymentDictionary[A]); }
                }

                //Remove the grand total tax from the account
                TR.Account.Balance -= TR.GrandTotalTax;

                //Send notifications to all the owners of this account
                foreach (User Owner in TR.Account.Owners) {

                    //Add the notification
                    //Create and add a notif
                    Notification N = new() {
                        Date = DateTime.Now, User = Owner,
                        Text = $"Neco has withdrawn {TR.Account.Name} ({TR.Account.ID})'s monthly tax of {TR.GrandTotalTax:n0}p. See your newest Tax Report for more details",
                    };

                    DB.Add(N);
                }

                //To the best of my knowledge this will also update the account
                DB.Add(TR);
            }

            //Now then that we have the total amount of tax to actually send the tax

            foreach (Account A in PaymentDictionary.Keys) {

                A.Balance += PaymentDictionary[A];

                //Send notifications to all the owners of this account
                foreach (User Owner in A.Owners) {

                    //Add the notification
                    //Create and add a notif
                    Notification N = new() {
                        Date = DateTime.Now, User = Owner,
                        Text = $"Neco has deposited {PaymentDictionary[A]:n0}p in tax for this month to {A.Name} ({A.ID}).",
                    };

                    DB.Add(N);
                }

                DB.Update(A);
            }

            await DB.SaveChangesAsync();
            return Ok();

        }

        #region Helpers

        /// <summary>Generates a report for a user</summary>
        /// <param name="DB">Neco Context to pull all data from</param>
        /// <param name="AccountID"></param>
        /// <returns></returns>
        [NonAction]
        internal static async Task<TaxReport> GenerateReport(NecoContext DB, string AccountID) {

            //Get account with districts
#pragma warning disable CS8602 // Dereference of a possibly null reference.
            Account? A = await DB.Account
                .Where(A=>A.Jurisdiction!=null) //This check should cover the null dereference
                .Include(A=>A.Jurisdiction).ThenInclude(A=>A.TiedAccount).ThenInclude(A=>A.Owners) //Do self joins require incldues?
                .Include(A => A.Jurisdiction).ThenInclude(D => D.ParentJurisdiction)
                .Include(A => A.Jurisdiction).ThenInclude(D => D.ChildJurisdictions)
                .Include(A => A.Jurisdiction).ThenInclude(D => D.Brackets)
                .Include(A => A.Airlines).ThenInclude(A => A.Jurisdiction).ThenInclude(D => D.ParentJurisdiction)
                .Include(A => A.Apartments).ThenInclude(A => A.Jurisdiction).ThenInclude(D => D.ParentJurisdiction)
                .Include(A => A.Businessses).ThenInclude(A => A.Jurisdiction).ThenInclude(D => D.ParentJurisdiction)
                .Include(A => A.Corporations).ThenInclude(A => A.Jurisdiction).ThenInclude(D => D.ParentJurisdiction)
                .Include(A => A.Hotels).ThenInclude(A => A.Jurisdiction).ThenInclude(D => D.ParentJurisdiction)
                .FirstOrDefaultAsync(A => A.ID == AccountID);
#pragma warning restore CS8602 // Dereference of a possibly null reference.

            //Governments and charities pay no taxes
            if (A is null || A.IncomeType == IncomeType.GOVERNMENT || A.IncomeType == IncomeType.CHARITY) { return TaxReport.Empty; }
            
            //Decide the time period we'll get transactions from:
            List<Transaction> Ts;

            if (DateTime.Now.Day > 15) {

                //Get Transactions since this month's 15th.
                Ts = await DB.Transaction
                    .Include(T => T.Origin).Include(T => T.Destination)
                    .Where(T => T.Origin !=  null && T.Destination != null && (T.Origin.ID == A.ID || T.Destination.ID == A.ID))
                    .Where(T => T.Date > DayOfThisMonth(15))
                    .ToListAsync();

            } else {
                //Get Transactions from the last 15th to this 15th

                Ts = await DB.Transaction
                    .Include(T => T.Origin).Include(T => T.Destination)
                    .Where(T => T.Origin != null && T.Destination != null && (T.Origin.ID == A.ID || T.Destination.ID == A.ID))
                    .Where(T => T.Date > DayOfLastMonth(15) && T.Date < DayOfThisMonth(15))
                    .ToListAsync();

            }

            return TaxReport.Create(A, Ts);

        }

        /// <summary>Checks if a given user is an administrator or not</summary>
        /// <returns></returns>
        [NonAction]
        private async Task<bool> IsAdmin(string UserID) => await DB.User.AnyAsync(U => U.ID == UserID && U.Roles.Admin);

        /// <summary>Gets a DateTime representing the specified day of last month (IE if it's currently December 5th, executing
        /// this function with param day=15, it would return November 15th). Use day 31 to get the last day of the last month.</summary>
        /// <param name="Day"></param>
        /// <returns></returns>
        private static DateTime DayOfLastMonth(int Day) {

            int LastMonth = DateTime.Now.Month - 1;
            int Year = DateTime.Now.Year;
            if (LastMonth == 0) {
                Year--;
                LastMonth = 12;
            }

            //The one condition this may fail in is if we ask for the last 31st and last month didn't have a 31st.
            //In this and similar instances (if the previous month does not have this date), let's use the month's maximum length.

            return new (Year, LastMonth, Math.Min(Day, DateTime.DaysInMonth(Year, LastMonth)));
        }

        /// <summary>Gets a DateTime representing the specified day of this month (IE if it's currently December 5th, executing
        /// this function with param day=15, it would return December 15th). Use day 31 to get the last day of this month.</summary>
        /// <param name="Day"></param>
        /// <returns></returns>
        private static DateTime DayOfThisMonth(int Day) => new (DateTime.Now.Year, DateTime.Now.Month, Math.Min(Day, DateTime.DaysInMonth(DateTime.Now.Year, DateTime.Now.Month)));

        #endregion

    }
}