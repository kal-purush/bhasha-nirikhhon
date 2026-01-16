using BudgetTracker.BudgetSquirrel.Application;
using BudgetTracker.BudgetSquirrel.Application.Messages;
using BudgetTracker.Business.Auth;
using BudgetTracker.Business.Budgeting;
using BudgetTracker.Business.Transactions;
using BudgetTracker.Business.Ports.Repositories;
using BudgetTracker.Data.EntityFramework;
using BudgetTracker.TestUtils.Seeds;
using BudgetTracker.TestUtils.Transactions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;

namespace BudgetTracker.BudgetSquirrel.WebApi.Tests.IntegrationTests
{
    public class TransactionTests : TestBase
    {
        private BasicSeed _seeder;
        private TransactionBuilderFactory _transactionBuilderFactory;
        private BudgetTrackerContext _dbContext;
        private IBudgetRepository _budgetRepository;
        private IUserRepository _userRepository;
        private ITransactionRepository _transactionRepository;

        public TransactionTests() : base()
        {
            _seeder = _startup.Services.GetService(typeof(BasicSeed)) as BasicSeed;
            _transactionBuilderFactory = _startup.Services.GetService(typeof(TransactionBuilderFactory)) as TransactionBuilderFactory;
            _dbContext = _startup.Server.Host.Services.GetService(typeof(BudgetTrackerContext)) as BudgetTrackerContext;
            _budgetRepository = _startup.Server.Host.Services.GetService(typeof(IBudgetRepository)) as IBudgetRepository;
            _userRepository = _startup.Server.Host.Services.GetService(typeof(IUserRepository)) as IUserRepository;
            _transactionRepository = _startup.Server.Host.Services.GetService(typeof(ITransactionRepository)) as ITransactionRepository;
        }

        [Fact]
        public async Task Test_BudgetFundUpdated_When_TransactionLogged()
        {
            Budget root;
            root = await _seeder.Seed(_userRepository, _budgetRepository);

            Budget budgetForTransaction = root.SubBudgets[1];
            TransactionMessage message = _transactionBuilderFactory.GetMessageBuilder()
                                                    .SetBudgetId(budgetForTransaction.Id)
                                                    .SetAmount(30)
                                                    .Build();

            string requestData = JsonConvert.SerializeObject(message);
            string messageStr = $"{{'user': {{ 'username': {root.Owner.Username}, 'password': {root.Owner.Password} }}," +
                                $"'arguments': {requestData} }}";
            ApiRequest request = (ApiRequest) JsonConvert.DeserializeObject(messageStr);
            Console.WriteLine(messageStr);
            HttpResponseMessage response = await _startup.SendJsonMessage(BudgetSquirrelUri.TRANSACTIONS_LOG, request, "POST");
        }
    }
}