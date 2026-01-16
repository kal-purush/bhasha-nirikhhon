using Microsoft.AspNetCore.Mvc;
using Igtampe.Neco.Common;
using Igtampe.Neco.Common.Income;
using Igtampe.Neco.Common.Banking;
using Igtampe.Neco.Data;
using Igtampe.Neco.API.Requests;
using Igtampe.ChopoSessionManager;
using Microsoft.EntityFrameworkCore;
using System.Reflection;
using Igtampe.Neco.Common.Taxes;

namespace Igtampe.Neco.API.Controllers {

    /// <summary>Sort by income</summary>
    public enum IncomeItemSortType { 
     
        /// <summary>Sort by name ascending</summary>
        NAME = 0,

        /// <summary>Sort by name descending</summary>
        NAME_DESC = 1,

        /// <summary>Sort by income descending</summary>
        INCOME = 2,

        /// <summary>Sort by income ascending</summary>
        INCOME_ASC = 3

    }

    /// <summary>Controller that handles User operations</summary>
    [Route("API/Income")]
    [ApiController]
    public class IncomeController : ControllerBase {

        private readonly NecoContext DB;

        /// <summary>Creates a User Controller</summary>
        /// <param name="Context"></param>
        public IncomeController(NecoContext Context) => DB = Context;

        #region Summary

        /// <summary>Gets a summary</summary>
        /// <param name="SessionID"></param>
        /// <param name="AccountID"></param>
        /// <returns></returns>
        [HttpGet]
        public async Task<ActionResult> Summary([FromHeader] Guid SessionID, [FromQuery] string AccountID) {

            //Check SessionID
            Session? S = await GetSession(SessionID);
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            bool isAccountOwner = await DB.Account.AnyAsync(A => A.ID == AccountID && A.Owners.Any(O => O.ID == S.UserID));
            if (!isAccountOwner && !await IsAdminOrSDC(S.UserID)) { return NotFound(ErrorResult.NotFound("Account was not found or is not owned by session owner", "AccountID")); }

            //Total income from all item subtypes individual and grand total

            //I really REALLY hope this maps, but I have a sneaking suspicion it won't
            long Airline = await DB.Airline.Where(I=>I.Account != null && I.Account.ID==AccountID).SumAsync(A=>A.Income());
            long Apartment = await DB.Apartment.Where(I => I.Account != null && I.Account.ID == AccountID).SumAsync(A => A.Income());
            long Business = await DB.Apartment.Where(I => I.Account != null && I.Account.ID == AccountID).SumAsync(A => A.Income());
            long Corporation = await DB.Corporation.Where(I => I.Account != null && I.Account.ID == AccountID).SumAsync(A => A.Income());
            long Hotel = await DB.Hotel.Where(I => I.Account != null && I.Account.ID == AccountID).SumAsync(A => A.Income());

            long Total = Airline + Apartment + Business + Corporation + Hotel;

            return Ok(new { Airline, Apartment, Business, Corporation, Hotel, Total });
        }

        #endregion

        #region IncomeDay
        
        /// <summary>Executes an Income Day event</summary>
        /// <param name="SessionID"></param>
        /// <param name="Force"></param>
        /// <returns></returns> 
        [HttpPost("IncomeDay")] //There's no real reason to make this a post, but I just want this to not be accessible via a standard web-browser
        public async Task<IActionResult> IncomeDay([FromHeader] Guid? SessionID, [FromQuery] bool? Force) {
            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized("Invalid Session"); }

            if (!await IsAdmin(S.UserID)) { return Forbid(); }

            if (DateTime.Now.Day != 15 && Force != true) { return BadRequest("It is not currently income day! If you wish to run income anyways, add Force=true"); }

            //Get *all* the accounts along with their income items
            List<Account> Accounts = await DB.Account
                .Include(A => A.Airlines).Include(A => A.Apartments)
                .Include(A => A.Businessses).Include(A => A.Corporations)
                .Include(A => A.Hotels).Include(A=>A.Owners).ToListAsync();

            foreach (Account A in Accounts) {

                //Get the total income
                long TotalIncome = A.IncomeItems.Sum(I => I.Income());

                //Deposit it
                A.Balance += TotalIncome; //Don't do a transaction. It'll be taxed twice.

                //Send a notification
                foreach (User Owner in A.Owners) {

                    //Add the notification
                    //Create and add a notif
                    Notification N = new() {
                        Date = DateTime.Now, User = Owner,
                        Text = $"Neco has deposited {A.Name} ({A.ID})'s monthly declared income of {TotalIncome:n0}p",
                    };

                    DB.Add(N);
                }

                DB.Update(A);
            }

            await DB.SaveChangesAsync();
            return Ok();

        }
        #endregion

        #region Get Collections

        /// <summary>Gets an account's Airlines</summary>
        /// <param name="SessionID"></param>
        /// <param name="AccountID"></param>
        /// <param name="Sort"></param>
        /// <param name="Query"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <returns></returns>
        [HttpGet("Airlines")]
        public async Task<IActionResult> GetAirlines([FromHeader] Guid SessionID, [FromQuery] string AccountID, [FromQuery] IncomeItemSortType? Sort,
            [FromQuery] string? Query, [FromQuery] int? Skip, [FromQuery] int? Take) => await GetIncomeItems(DB.Airline, SessionID, AccountID, Sort, Query, Skip, Take);

        /// <summary>Gets an account's apartments</summary>
        /// <param name="SessionID"></param>
        /// <param name="AccountID"></param>
        /// <param name="Sort"></param>
        /// <param name="Query"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <returns></returns>
        [HttpGet("Apartments")]
        public async Task<IActionResult> GetApartments([FromHeader] Guid SessionID, [FromQuery] string AccountID, [FromQuery] IncomeItemSortType? Sort,
            [FromQuery] string? Query, [FromQuery] int? Skip, [FromQuery] int? Take) => await GetIncomeItems(DB.Apartment, SessionID, AccountID, Sort, Query, Skip, Take);

        /// <summary>Gets an account's Businesses</summary>
        /// <param name="SessionID"></param>
        /// <param name="AccountID"></param>
        /// <param name="Sort"></param>
        /// <param name="Query"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <returns></returns>
        [HttpGet("Businesses")]
        public async Task<IActionResult> GetBusinesses([FromHeader] Guid SessionID, [FromQuery] string AccountID, [FromQuery] IncomeItemSortType? Sort,
            [FromQuery] string? Query, [FromQuery] int? Skip, [FromQuery] int? Take) => await GetIncomeItems(DB.Business, SessionID, AccountID, Sort, Query, Skip, Take);

        /// <summary>Gets an account's corporations</summary>
        /// <param name="SessionID"></param>
        /// <param name="AccountID"></param>
        /// <param name="Sort"></param>
        /// <param name="Query"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <returns></returns>
        [HttpGet("Corporations")]
        public async Task<IActionResult> GetCorporations([FromHeader] Guid SessionID, [FromQuery] string AccountID, [FromQuery] IncomeItemSortType? Sort,
            [FromQuery] string? Query, [FromQuery] int? Skip, [FromQuery] int? Take) => await GetIncomeItems(DB.Corporation, SessionID, AccountID, Sort, Query, Skip, Take);

        /// <summary>Gets an account's Hotels</summary>
        /// <param name="SessionID"></param>
        /// <param name="AccountID"></param>
        /// <param name="Sort"></param>
        /// <param name="Query"></param>
        /// <param name="Skip"></param>
        /// <param name="Take"></param>
        /// <returns></returns>
        [HttpGet("Hotels")]
        public async Task<IActionResult> GetHotels([FromHeader] Guid SessionID, [FromQuery] string AccountID, [FromQuery] IncomeItemSortType? Sort,
            [FromQuery] string? Query, [FromQuery] int? Skip, [FromQuery] int? Take) => await GetIncomeItems(DB.Hotel, SessionID, AccountID, Sort, Query, Skip, Take);

        #endregion

        #region Get Individual

        /// <summary>Gets specific airline</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpGet("Airlines/{ItemID}")]
        public async Task<IActionResult> GetAirline([FromHeader] Guid SessionID, [FromRoute] Guid ItemID) 
            => await GetIncomeItem(DB.Airline, SessionID, ItemID);

        /// <summary>Gets specific Apartment</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpGet("Apartments/{ItemID}")]
        public async Task<IActionResult> GetApartment([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await GetIncomeItem(DB.Apartment, SessionID, ItemID);

        /// <summary>Gets a specific Business</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpGet("Businesses/{ItemID}")]
        public async Task<IActionResult> GetBusiness([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await GetIncomeItem(DB.Business, SessionID, ItemID);

        /// <summary>Gets specific Corporation</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpGet("Corporations/{ItemID}")]
        public async Task<IActionResult> GetCorporation([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await GetIncomeItem(DB.Corporation, SessionID, ItemID);

        /// <summary>Gets a specific Hotel</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpGet("Hotels/{ItemID}")]
        public async Task<IActionResult> GetHotel([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await GetIncomeItem(DB.Hotel, SessionID, ItemID);

        #endregion

        #region Create Individual

        /// <summary>Creates an Airline</summary>
        /// <param name="SessionID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPost("Airlines")]
        public async Task<IActionResult> CreateAirline([FromHeader] Guid SessionID, [FromBody] AirlineRequest Request)
            => await CreateIncomeItem<Airline,AirlineRequest>(SessionID, Request);

        /// <summary>Creates an apartment</summary>
        /// <param name="SessionID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPost("Apartments")]
        public async Task<IActionResult> CreateApartment([FromHeader] Guid SessionID, [FromBody] ApartmentRequest Request)
            => await CreateIncomeItem<Apartment,ApartmentRequest>(SessionID, Request);

        /// <summary>Creates a Business</summary>
        /// <param name="SessionID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPost("Businesses")]
        public async Task<IActionResult> CreateBusiness([FromHeader] Guid SessionID, [FromBody] BusinessRequest Request)
            => await CreateIncomeItem<Business,BusinessRequest>(SessionID, Request);

        /// <summary>Creates a corporation</summary>
        /// <param name="SessionID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPost("Corporations")]
        public async Task<IActionResult> CreateCorporation([FromHeader] Guid SessionID, [FromBody] CorporationRequest<Corporation> Request)
            => await CreateIncomeItem<Corporation,CorporationRequest<Corporation>>(SessionID, Request);

        /// <summary>Creates a hotel</summary>
        /// <param name="SessionID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPost("Hotels")]
        public async Task<IActionResult> CreateHotel([FromHeader] Guid SessionID, [FromBody] HotelRequest Request)
            => await CreateIncomeItem<Hotel,HotelRequest>(SessionID, Request);

        #endregion

        #region Update Individual

        /// <summary>Updates an Airline</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPut("Airlines/{ItemID}")]
        public async Task<IActionResult> UpdateAirline([FromHeader] Guid SessionID, [FromRoute] Guid ItemID, [FromBody] AirlineRequest Request)
            => await UpdateIncomeItem(DB.Airline, SessionID, ItemID, Request);

        /// <summary>Updates an apartment</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPut("Apartments/{ItemID}")]
        public async Task<IActionResult> UpdateApartment([FromHeader] Guid SessionID, [FromRoute] Guid ItemID, [FromBody] ApartmentRequest Request)
            => await UpdateIncomeItem(DB.Apartment, SessionID, ItemID, Request);

        /// <summary>Updates a Business</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPut("Businesses/{ItemID}")]
        public async Task<IActionResult> UpdateBusiness([FromHeader] Guid SessionID, [FromRoute] Guid ItemID, [FromBody] BusinessRequest Request)
            => await UpdateIncomeItem(DB.Business, SessionID, ItemID, Request);

        /// <summary>Updates a corporation</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPut("Corporations/{ItemID}")]
        public async Task<IActionResult> UpdateCorporation([FromHeader] Guid SessionID, [FromRoute] Guid ItemID, [FromBody] CorporationRequest<Corporation> Request)
            => await UpdateIncomeItem(DB.Corporation, SessionID, ItemID, Request);

        /// <summary>Updates a hotel</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <param name="Request"></param>
        /// <returns></returns>
        [HttpPut("Hotels/{ItemID}")]
        public async Task<IActionResult> UpdateHotel([FromHeader] Guid SessionID, [FromRoute] Guid ItemID, [FromBody] HotelRequest Request)
            => await UpdateIncomeItem(DB.Hotel, SessionID, ItemID, Request);

        #endregion

        #region Delete Individual

        /// <summary>Deletes specific airline</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpDelete("Airlines/{ItemID}")]
        public async Task<IActionResult> DeleteAirline([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await DeleteIncomeItem(DB.Airline, SessionID, ItemID);

        /// <summary>Deletes specific Apartment</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpDelete("Apartments/{ItemID}")]
        public async Task<IActionResult> DeleteApartment([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await DeleteIncomeItem(DB.Apartment, SessionID, ItemID);

        /// <summary>Deletes a specific Business</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpDelete("Businesses/{ItemID}")]
        public async Task<IActionResult> DeleteBusiness([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await DeleteIncomeItem(DB.Business, SessionID, ItemID);

        /// <summary>Deletes specific Corporation</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpDelete("Corporations/{ItemID}")]
        public async Task<IActionResult> DeleteCorporation([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await DeleteIncomeItem(DB.Corporation, SessionID, ItemID);

        /// <summary>Deletes a specific Hotel</summary>
        /// <param name="SessionID"></param>
        /// <param name="ItemID"></param>
        /// <returns></returns>
        [HttpDelete("Hotels/{ItemID}")]
        public async Task<IActionResult> DeleteHotel([FromHeader] Guid SessionID, [FromRoute] Guid ItemID)
            => await DeleteIncomeItem(DB.Hotel, SessionID, ItemID);

        #endregion

        #region Generic Functions

        /// <summary>Generic function to get any type of income item list</summary>
        /// <typeparam name="E">Type of income item list to retrieve</typeparam>
        /// <param name="BaseSet">Set to take IncomeItems from</param>
        /// <param name="SessionID">ID of the session executing this request</param>
        /// <param name="Sort">Sort order the item list</param>
        /// <param name="Query">Search query to search the name or description of item type</param>
        /// <param name="Skip">How many items to skip</param>
        /// <param name="Take">How many items to take</param>
        /// <param name="AccountID">Account ID to get </param>
        /// <returns></returns>
        private async Task<IActionResult> GetIncomeItems<E>(IQueryable<E> BaseSet, Guid? SessionID, string? AccountID,
            IncomeItemSortType? Sort, string? Query, int? Skip, int? Take) where E : IncomeItem {

            Session? S = await GetSession(SessionID);
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            bool isAccountOwner = await DB.Account.AnyAsync(A => A.ID == AccountID && A.Owners.Any(O => O.ID == S.UserID));
            if (!isAccountOwner && ! await IsAdminOrSDC(S.UserID)) { return NotFound(ErrorResult.NotFound("Account was not found or is not owned by session owner", "AccountID")); }

            BaseSet = BaseSet.Where(I => I.Account != null && I.Account.ID == AccountID);
            if (!string.IsNullOrWhiteSpace(Query)) { BaseSet = BaseSet.Where(I => I.Name.ToLower().Contains(Query.ToLower()) || I.Description.ToLower().Contains(Query.ToLower())); }

            BaseSet = Sort switch {
                IncomeItemSortType.NAME_DESC => BaseSet.OrderByDescending(I => I.Name),
                IncomeItemSortType.INCOME => BaseSet.OrderByDescending(I => I.Income()), //This may not map
                IncomeItemSortType.INCOME_ASC => BaseSet.OrderBy(I => I.Income()),
                _ => BaseSet.OrderBy(I => I.Name),
            };

            BaseSet.Skip(Skip ?? 0).Take(Take ?? 20);

            return Ok(await BaseSet.ToListAsync());
        }

        /// <summary>Generic function to get any type of income item</summary>
        /// <typeparam name="E"></typeparam>
        /// <param name="BaseSet"></param>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        private async Task<IActionResult> GetIncomeItem<E>(IQueryable<E> BaseSet, Guid? SessionID, Guid? ID) where E : IncomeItem {

            Session? S = await GetSession(SessionID);
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            //Ok this should be simple
            E? Item = await GetItem(BaseSet,S,ID);

            return Item is null
                ? NotFound(ErrorResult.NotFound("Item was not found or is not owned by account owned by session owner", "ID"))
                : Ok(Item);
        }

        /// <summary>Generic function to create any type of income item</summary>
        /// <typeparam name="E"></typeparam>
        /// <typeparam name="F"></typeparam>
        /// <param name="SessionID"></param>
        /// <param name="ItemRequest"></param>
        /// <returns></returns>
        private async Task<IActionResult> CreateIncomeItem<E, F>(Guid? SessionID, F ItemRequest) where E : IncomeItem, new() where F : IncomeItemRequest<E> {

            Session? S = await GetSession(SessionID);
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            E Item = new();

            foreach (PropertyInfo Prop in typeof(F).GetProperties()) {

                //Check for the properties we need to skip
                switch (Prop.Name.ToUpper()) {
                    case "ID":
                        //Ignore ID
                        continue;
                    case "JURISDICTIONID":
                        //Look for the District ID then keep going

                        Jurisdiction? J = await DB.Jurisdiction.FindAsync(ItemRequest.JurisdictionID);
                        if (J is null) { return NotFound(ErrorResult.NotFound("Jurisdiction was not found","Jurisdiction")); }
                        Item.Jurisdiction = J;

                        continue;

                    case "ACCOUNTID":
                        //Look for the account ID then keep going

                        Account? A = await DB.Account.FirstOrDefaultAsync(A => A.ID == ItemRequest.AccountID && A.Owners.Any(O => O.ID == S.UserID));
                        if (A is null) { return NotFound(ErrorResult.NotFound("Account was not found or is not owned by session owner", "Account")); }
                        Item.Account = A;

                        continue;
                    default:
                        break;
                }

                //Get the updated value
                object? O = Prop.GetValue(ItemRequest);

                //Update the value as long as its not null
                if (O is not null) { Prop.SetValue(Item, O); }
            }

            DB.Add(Item);
            await DB.SaveChangesAsync();

            return Ok(Item);

        }

        /// <summary>Generic function to update any type of income item</summary>
        /// <typeparam name="E"></typeparam>
        /// <typeparam name="F"></typeparam>
        /// <param name="BaseSet"></param>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <param name="ItemRequest"></param>
        /// <returns></returns>
        private async Task<IActionResult> UpdateIncomeItem<E, F>(IQueryable<E> BaseSet, Guid? SessionID, Guid? ID, F ItemRequest) where E : IncomeItem where F : IncomeItemRequest<E> {

            Session? S = await GetSession(SessionID);
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            E? Item = await GetItem(BaseSet, S, ID);
            if (Item is null) { return NotFound(ErrorResult.NotFound("Item was not found or is not owned by account owned by session owner","ID")); }

            foreach (PropertyInfo Prop in typeof(F).GetProperties()) {

                //Check for the properties we need to skip
                switch (Prop.Name.ToUpper()) {
                    case "ID":
                        //Ignore ID
                        continue;
                    case "JURISDICTIONID":
                        //Look for the District ID then keep going

                        Jurisdiction? J = await DB.Jurisdiction.FindAsync(ItemRequest.JurisdictionID);
                        if (J is null) { return NotFound(ErrorResult.NotFound("Jurisdiction was not found", "Jurisdiction")); }
                        Item.Jurisdiction = J;

                        continue;

                    case "ACCOUNTID":
                        //Look for the account ID then keep going

                        Account? A = await DB.Account.FirstOrDefaultAsync(A => A.ID == ItemRequest.AccountID && A.Owners.Any(O => O.ID == S.UserID));
                        if (A is null) { return NotFound(ErrorResult.NotFound("Account was not found or is not owned by session owner", "Account")); }
                        Item.Account = A;

                        continue;
                    default:
                        break;
                }

                //Get the updated value
                object? O = Prop.GetValue(ItemRequest);

                //Update the value as long as its not null
                if (O is not null) { Prop.SetValue(Item, O); }
            }

            DB.Add(Item);
            await DB.SaveChangesAsync();

            return Ok(Item);

        }

        /// <summary>Generic function to delete any type of income item</summary>
        /// <typeparam name="E"></typeparam>
        /// <param name="BaseSet"></param>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        private async Task<IActionResult> DeleteIncomeItem<E>(IQueryable<E> BaseSet, Guid? SessionID, Guid? ID) where E : IncomeItem {

            Session? S = await GetSession(SessionID);
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            E? Item = await GetItem(BaseSet, S, ID);
            if (Item is null) { return NotFound(ErrorResult.NotFound("Item was not found or is not owned by account owned by session owner", "ID")); }

            DB.Remove(Item);
            await DB.SaveChangesAsync();

            return Ok(Item);

        }

        #endregion

        /// <summary>Actually gets an item (verifying if the user has access)</summary>
        /// <typeparam name="E"></typeparam>
        /// <param name="BaseSet"></param>
        /// <param name="S"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        private async Task<E?> GetItem<E>(IQueryable<E> BaseSet, Session S, Guid? ID) where E : IncomeItem{

            BaseSet = BaseSet.Include(I => I.Jurisdiction);

            return await IsAdminOrSDC(S.UserID) 
                ? await BaseSet.FirstOrDefaultAsync(I=>I.ID==ID) 
                : await BaseSet.FirstOrDefaultAsync(I => I.ID == ID && I.Account != null && I.Account.Owners.Any(O => O.ID == S.UserID));

        }

        /// <summary>Checks if given user is either an administrator, or a member of the Salary Determination Committee</summary>
        /// <param name="UserID"></param>
        /// <returns></returns>
        private async Task<bool> IsAdminOrSDC(string UserID) => await DB.User.AnyAsync(U => U.ID == UserID && (U.Roles.Admin || U.Roles.SDC));

        /// <summary>Checks if given user is an administrator</summary>
        /// <param name="UserID"></param>
        /// <returns></returns>
        private async Task<bool> IsAdmin(string UserID) => await DB.User.AnyAsync(U => U.ID == UserID && (U.Roles.Admin));

        /// <summary>Gets a session asynchronously</summary> //(this should maybe just be a function within the sesion manager)
        /// <param name="SessionID"></param>
        /// <returns></returns>
        private static async Task<Session?> GetSession(Guid? SessionID) => await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));

    }
}