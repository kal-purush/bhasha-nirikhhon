using Bank.Application.Requests;

namespace Bank.UserService.Test.Examples.Entities;

public static partial class Example
{
    public static partial class Entity
    {
        public static class AccountCurrency
        {
            public static readonly AccountCurrencyCreateRequest CreateRequest = Database.Examples.Example.AccountCurrency.CreateRequest;

            public static readonly AccountCurrencyClientUpdateRequest ClientUpdateRequest = Database.Examples.Example.AccountCurrency.ClientUpdateRequest;

            public static readonly Guid AccountCurrencyId = Database.Seeders.Seeder.AccountCurrency.BankEuro.Id;

        }
    }
}