using DaBank.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace DaBank.Repository
{
    public interface IBankRepository
    {
        Bank GetBank();
        Account GetAccount(int account);
        void SaveBank(Bank bank);
    }

    public class BankRepository : IBankRepository
    {

        private Bank _bank;
        public BankRepository()
        {
            _bank = new Bank();
            CreateBankRep();
        }

        public Bank GetBank()
        {           
            return _bank;
        }

        public Account GetAccount(int account)
        {

            return GetBank().Customers.SelectMany(x => x.Account.Where(y => y.AccountNumber == account)).FirstOrDefault();
        }

        public void SaveBank(Bank bank)
        {
            _bank = bank;
        }

        private void CreateBankRep()
        {
            var bankRep = new Bank();

            bankRep.Customers = new List<Customer>
            {
                new Customer("David Agdelius", 1, new List<Account>
                {
                    new Account(10, 10000),
                    new Account(11, 400)
                }),
                new Customer("Thorleif Svensson", 2, new List<Account>
                {
                    new Account(222, 5000)
                }),
                new Customer("Ronny Karlsson", 3, new List<Account>
                {
                    new Account(34, 5000000)
                }),
                new Customer("Eskil Knutsson", 4, new List<Account>
                {
                    new Account(21, 100)
                })
            };

            _bank = bankRep;
        }
    }
}