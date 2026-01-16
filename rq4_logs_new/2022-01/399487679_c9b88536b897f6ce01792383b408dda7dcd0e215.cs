using Microsoft.AspNetCore.Mvc;
using Igtampe.Neco.API.Requests;
using Igtampe.Neco.Common;
using Igtampe.Neco.Common.Banking;
using Igtampe.Neco.Common.Income;
using Igtampe.Neco.Data;
using Igtampe.ChopoSessionManager;
using Microsoft.EntityFrameworkCore;
using Igtampe.Neco.Common.Taxes;

namespace Igtampe.Neco.API.Controllers {

    /// <summary>Type of transaction from the perspective of an executor</summary>
    public enum TransactionType {

        /// <summary>Indicates any transaction where the executor is either origin or destination</summary>
        ANY = -1,

        /// <summary>Indicates a transaction where from the executor's perspective, they are the origin</summary>
        DEBIT = 0,

        /// <summary>Indicates a transaction where from the executor's perspective, they are the destination</summary>
        CREDIT = 1
    }

    /// <summary>Type of sort to execute on transactions</summary>
    public enum TransactionSortType { 

        /// <summary>Sort by Date (Descending)</summary>
        DATE = 0,
        
        /// <summary>Sort by Date (Ascending)</summary>
        DATE_ASC = 1,

        /// <summary>Sort by Amount (descending)</summary>
        AMOUNT = 2,

        /// <summary>Sort by Amount (Ascending)</summary>
        AMOUNT_ASC = 3,

    }

    /// <summary>Controller that handles User operations</summary>
    [Route("API/Bank")]
    [ApiController]
    public class BankController : ControllerBase {

        private readonly NecoContext DB;

        /// <summary>Creates a User Controller</summary>
        /// <param name="Context"></param>
        public BankController(NecoContext Context) => DB = Context;

        /// <summary>Gets all banks</summary>
        /// <param name="Query">Search query to search the ID or name of a bank</param>
        /// <param name="Skip">Banks to skip</param>
        /// <param name="Take">Amount of banks to return</param>
        /// <returns></returns>
        [HttpGet]
        public async Task<IActionResult> GetBanks(string? Query, int? Skip, int? Take) {
            IQueryable<Bank> Banks = DB.Bank;
            if (!string.IsNullOrWhiteSpace(Query)) { Banks = Banks.Where(B => B.Name.ToLower().Contains(Query.ToLower()) || (B.ID != null && B.ID.ToLower().Contains(Query.ToLower()))); }
            Banks = Banks.Skip(Skip ?? 0).Take(Take ?? 20);
            return Ok(await Banks.ToListAsync());
        }

        /// <summary>Gets a specific Bank</summary>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("{ID}")]
        public async Task<IActionResult> GetBank(string ID) {
            Bank? B = await DB.Bank.FirstOrDefaultAsync(B => B.ID == ID);
            return B is null ? NotFound(ErrorResult.NotFound("Bank was not found", "ID")) : Ok(B);
        }

        /// <summary>Get all accounts owned by a session owner</summary>
        /// <param name="SessionID"></param>
        /// <returns></returns>
        [HttpGet("Accounts")]
        public async Task<IActionResult> GetAccounts([FromHeader] Guid? SessionID) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            //Get the user
            return Ok(await DB.Account.Include(A => A.Bank).Include(A => A.Jurisdiction).Where(A => A.Owners != null && A.Owners.Any(O => O.ID == S.UserID) && !A.Closed).ToListAsync());

        }

        /// <summary>Gets a specific account from the list of accoutns owned by the session owner</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("Accounts/{ID}")]
        public async Task<IActionResult> GetAccount([FromHeader] Guid? SessionID, string ID) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            Account? A = await DB.Account.Include(A=>A.Bank).Include(A => A.Jurisdiction).FirstOrDefaultAsync(A => A.Owners.Any(U => U.ID == S.UserID) && A.ID == ID);
            return A is null ? NotFound(ErrorResult.NotFound("Bank account was not found or is not accessible by session owner")) : Ok(A);
        }

        /// <summary>Gets the accounts directory</summary>
        /// <param name="Query"></param>
        /// <param name="Type"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <returns></returns>
        [HttpGet("Accounts/Dir")]
        public async Task<IActionResult> AccountDirectory([FromQuery] string? Query, [FromQuery] IncomeType? Type, [FromQuery] int? Skip, [FromQuery] int? Take) {

            IQueryable<Account> BaseSet = DB.Account.Include(A => A.Bank).Include(A => A.Jurisdiction).Where(A => A.PubliclyListed && !A.Closed);
            if (!string.IsNullOrWhiteSpace(Query)) { BaseSet = BaseSet.Where(I => I.Name.ToLower().Contains(Query.ToLower())); }
            if (Type != null) { BaseSet = BaseSet.Where(I => I.IncomeType == Type); }
            BaseSet.Skip(Skip ?? 0).Take(Take ?? 20);

            List<Account> Accounts = await BaseSet.ToListAsync();
            Accounts.ForEach(A => A.Balance = 0); //Clear balances for *privacy*

            //Now go ahead and put it out there
            return Ok(Accounts);

        }

        /// <summary>Gets Transactions for a given account</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <param name="Query"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <param name="Type"></param>
        /// <param name="Start"></param>
        /// <param name="End"></param>
        /// <param name="Sort"></param>
        /// <returns></returns>
        [HttpGet("Accounts/{ID}/Transactions")]
        public async Task<IActionResult> GetAccountTransactions([FromHeader] Guid? SessionID, string ID,
            [FromQuery] string? Query, [FromQuery] int? Skip, [FromQuery] int? Take, [FromQuery] TransactionType? Type,
            [FromQuery] DateTime? Start, [FromQuery] DateTime? End, [FromQuery] TransactionSortType? Sort) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            IQueryable<Transaction> set = DB.Transaction;
            set = Type switch {
                TransactionType.DEBIT => set.Where(T => T.Origin != null && T.Origin.ID == ID), //Only origin
                TransactionType.CREDIT => set.Where(T => T.Destination != null && T.Destination.ID == ID), //Only destination
                _ => set.Where(T => T.Origin != null && T.Destination != null && (T.Origin.ID == ID || T.Destination.ID == ID)), //Both
            };

            if (!string.IsNullOrWhiteSpace(Query)) { set = set.Where(S => S.Name.ToLower().Contains(Query.ToLower())); }
            if (Start is not null) { set = set.Where(S => S.Date > Start); }
            if (End is not null) { set = set.Where(S => S.Date < End); }

            set = Sort switch {
                TransactionSortType.AMOUNT_ASC => set = set.OrderBy(A => A.Amount), //Amount Ascending
                TransactionSortType.DATE => set = set.OrderByDescending(A => A.Date), //Date descending
                TransactionSortType.DATE_ASC => set = set.OrderBy(A => A.Date), //Date ascending
                _ => set = set.OrderByDescending(A => A.Amount), //Amount descending (Default)
            };

            set = set.Skip(Skip ?? 0).Take(Take ?? 20);

            return Ok(await set.ToListAsync());
        }

        //Banks are the only types we allow to specify ID rather than having it generated

        /// <summary>Creates a Bank</summary>
        /// <param name="SessionID">ID of the administrator session that is executing this request</param>
        /// <param name="Request">Request with Bank details</param>
        /// <param name="ID">ID of the bank to create</param>
        /// <returns></returns>
        [HttpPost("{ID}")]
        public async Task<IActionResult> CreateBank([FromHeader] Guid? SessionID, [FromBody] BankRequest Request, string ID) {

            if (await DB.Bank.AnyAsync(B => B.ID == ID)) { return BadRequest(ErrorResult.BadRequest($"Bank with ID {ID} already exists!","ID")); }

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            bool IsAdmin = await DB.User.AnyAsync(U => U.ID == S.UserID && U.Roles.Admin);
            if (!IsAdmin) { return Forbid(); }

            Bank B = new() { ID=ID, Name = Request.Name, ImageURL = Request.ImageURL };

            DB.Add(B);
            await DB.SaveChangesAsync();

            return Ok(B);

        }

        /// <summary>Creates an account with given details</summary>
        /// <param name="SessionID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPost("Accounts")]
        public async Task<IActionResult> CreateAccount([FromHeader] Guid? SessionID, [FromBody] AccountRequest Request) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            User? U = await DB.User.FindAsync(S.UserID);
            if (U is null) { return NotFound(ErrorResult.NotFound("User was not found", "User")); }

            Jurisdiction? D = await DB.Jurisdiction.FindAsync(Request.DistrictID);
            if (D is null) { return NotFound(ErrorResult.NotFound("District was not found","District")); }

            Bank? B = await DB.Bank.FindAsync(Request.BankID);
            if(B is null) { return NotFound(ErrorResult.NotFound("Bank was not found", "Bank")); }

            Account A = new() { 
                Name = Request.Name, Address = Request.Address, PubliclyListed = Request.PubliclyListed,
                Balance = 0, Jurisdiction = D, Owners = new(), Bank = B,
            };

            A.Owners.Add(U);

            do { A.ID = A.IDGenerator.Generate(); } while (await DB.Account.AnyAsync(a => a.ID == A.ID));

            DB.Add(A);
            await DB.SaveChangesAsync();

            return Ok(A);

        }

        /// <summary>Creates and processes a transaction</summary>
        /// <param name="SessionID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPost("Transaction")]
        public async Task<IActionResult> CreateTransaction([FromHeader] Guid? SessionID, [FromBody] TransactionRequest Request) {

            if(Request.Amount < 0) { return BadRequest(ErrorResult.BadRequest("Amount cannot be less than 0","Amount")); }

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            Account? Origin = await DB.Account.Include(A=>A.Owners).FirstOrDefaultAsync(A=>A.ID==Request.Origin && A.Owners.Any(O=>O.ID==S.UserID) && !A.Closed);
            if(Origin is null) { return NotFound(ErrorResult.NotFound("Origin was not found or does not belong to the session owner","Origin")); }
            if (Origin.Balance < Request.Amount) { return BadRequest(ErrorResult.BadRequest("Insufiscient Funds","Origin")); }

            Account? Destination = await DB.Account.Include(A => A.Owners).FirstOrDefaultAsync(A=>A.ID==Request.Destination && !A.Closed);
            if(Destination is null) { return NotFound(ErrorResult.NotFound("Destination was not found","Destination")); }

            Transaction T = new() {
                Amount = Request.Amount,
                Date = DateTime.Now,
                Destination = Destination,
                Name = Request.Name,
                Origin = Origin
            };

            //Process the Transaction
            T.Origin.Balance -= Request.Amount;
            T.Destination.Balance += Request.Amount;

            DB.Add(T);

            //Add a notification to each owner of each de-esta cosa
            foreach (User O in T.Destination.Owners) {

                //Create and add a notif
                Notification N = new() {
                    Date = DateTime.Now, User = O,
                    Text = $"Account {T.Destination.Name} ({T.Destination.ID}) Received {T.Amount:N0}p from {T.Origin.ID}",
                };

                DB.Add(N);

            }

            await DB.SaveChangesAsync();

            //Clear for privacy
            T.Origin.Balance = 0;
            T.Destination.Balance = 0;

            return Ok(T);

        }

        /// <summary>Edits a Bank</summary>
        /// <param name="SessionID">ID of an administrator session</param>
        /// <param name="ID">ID of the bank</param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPut("{ID}")]
        public async Task<IActionResult> EditBank([FromHeader] Guid? SessionID, string ID, BankRequest Request) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            bool IsAdmin = await DB.User.AnyAsync(U => U.ID == S.UserID && U.Roles.Admin);
            if (!IsAdmin) { return Forbid(); }

            Bank? B = await DB.Bank.FindAsync(ID);
            if(B is null) { return NotFound(ErrorResult.NotFound("Bank was not found","Bank")); }

            B.Name = Request.Name;
            B.ImageURL= Request.ImageURL; 

            DB.Update(B);
            await DB.SaveChangesAsync();

            return Ok(B);

        }

        /// <summary>Edits an account</summary>
        /// <param name="SessionID">ID of the session executing this request</param>
        /// <param name="ID">ID of the account</param>
        /// <param name="Request">Request to edit account details</param>
        /// <returns></returns>
        [HttpPut("Accounts/{ID}")]
        public async Task<IActionResult> EditAccount([FromHeader] Guid? SessionID, string ID, [FromBody] AccountRequest Request) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            User? U = await DB.User.FindAsync(S.UserID);
            if (U is null) { return NotFound(ErrorResult.NotFound("User was not found", "User")); }

            Jurisdiction? D = await DB.Jurisdiction.FindAsync(Request.DistrictID);
            if (D is null) { return NotFound(ErrorResult.NotFound("District was not found", "District")); }

            //Bank? B = await DB.Bank.FindAsync(Request.BankID);
            //if (B is null) { return NotFound(ErrorResult.NotFound("Bank was not found", "Bank")); } //Bank is ignored

            Account? A = await DB.Account.FirstOrDefaultAsync(A=> A.ID==ID && A.Owners.Any(O=>O.ID==S.UserID));
            if (A is null) { return NotFound(ErrorResult.NotFound("Account was not found, or is not owned by the given user","Account")); }

            A.Name=Request.Name;
            A.Address=Request.Address;
            A.PubliclyListed=Request.PubliclyListed;
            A.Jurisdiction = D;

            DB.Update(A);
            await DB.SaveChangesAsync();

            return Ok(A);
        }

        /// <summary>Adds an additional owner to a given account</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <param name="Owner"></param>
        /// <returns></returns>
        [HttpPut("Accounts/{ID}/AddOwner")]
        public async Task<IActionResult> AddOwner([FromHeader] Guid? SessionID, string ID, [FromQuery] string? Owner) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            if (S.UserID == Owner) { return BadRequest(ErrorResult.BadRequest("Cannot add yourself to an account!", "Owner")); }

            User? U = await DB.User.FindAsync(Owner);
            if (U is null) { return NotFound(ErrorResult.NotFound("User was not found", "User")); }

            Account? A = await DB.Account.Include(A=>A.Owners).FirstOrDefaultAsync(A => A.ID == ID && A.Owners.Any(O => O.ID == S.UserID));
            if (A is null) { return NotFound(ErrorResult.NotFound("Account was not found, or is not owned by the given user", "Account")); }

            if (A.Owners.Contains(U)) { return BadRequest(ErrorResult.BadRequest("Given user is already an owner","Owner")); }
            A.Owners.Add(U);

            DB.Update(A);
            await DB.SaveChangesAsync();

            return Ok(A);

        }

        /// <summary>Removes an additional owner to a given account</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <param name="Owner"></param>
        /// <returns></returns>
        [HttpPut("Accounts/{ID}/RemoveOwner")]
        public async Task<IActionResult> RemoveOwner([FromHeader] Guid? SessionID, string ID, [FromQuery] string? Owner) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            if (S.UserID == Owner) { return BadRequest(ErrorResult.BadRequest("Cannot remove yourself from an account!","Owner")); }

            User? U = await DB.User.FindAsync(Owner);
            if (U is null) { return NotFound(ErrorResult.NotFound("User was not found", "User")); }

            Account? A = await DB.Account.Include(A => A.Owners).FirstOrDefaultAsync(A => A.ID == ID && A.Owners.Any(O => O.ID == S.UserID));
            if (A is null) { return NotFound(ErrorResult.NotFound("Account was not found, or is not owned by the given user", "Account")); }

            if (!A.Owners.Contains(U)) { return BadRequest(ErrorResult.BadRequest("Given user is not an owner", "Owner")); }
            if (A.Owners.Count==1) { return BadRequest(ErrorResult.BadRequest("Cannot remove only account owner", "Owner")); }
            A.Owners.Remove(U);

            DB.Update(A);
            await DB.SaveChangesAsync();

            return Ok(A);

        }

        /// <summary>Closes a Neco bank account</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpDelete("Accounts/{ID}")]
        public async Task<IActionResult> CloseAccount([FromHeader] Guid? SessionID, string ID) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            Account? A = await DB.Account.FirstOrDefaultAsync(A => A.ID == ID && A.Owners.Any(O => O.ID == S.UserID));
            if (A is null) { return NotFound(ErrorResult.NotFound("Account was not found, or is not owned by the given user", "Account")); }

            if (A.Balance > 0) { return BadRequest(ErrorResult.BadRequest("Account is not empty! Empty it out before closing the account!","Account")); }
            A.Closed = true;

            DB.Update(A);
            await DB.SaveChangesAsync();

            return Ok(A);

        }
    }
}