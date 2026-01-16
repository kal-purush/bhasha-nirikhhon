using System;
using System.Text;

namespace Lesson_02
{
    public enum AccountType
    {
        Undefined,
        Current,
        Credit,
        Deposit
    }

    class AccountTask01
    {
        // Создать класс счет в банке с закрытыми полями: номер счета, баланс, тип банковского счета (использовать перечислимый тип).
        // Предусмотреть методы для доступа к данным – заполнения и чтения.
        // Создать объект класса, заполнить его поля и вывести информацию об объекте класса на печать


        UInt64 _number;
        Decimal _balance;
        AccountType _type;

 
        public UInt64 CreateAccount(UInt64 accNumber)
        {
            _number = accNumber;
            return _number;
        }


        public UInt64 GetAccountNumber()
        {
            return _number;
        }

        public decimal PutTheMoney(decimal amount)
        {
            if (_number != 0)
            {
                _balance += amount;
                return _balance;
            }

            throw new Exception("Account doesn't exist");
        }

        public decimal WithdrawMoney(decimal amount)
        {
            if (_number != 0)
            {
                if (amount > _balance)
                {
                    throw new Exception($"The withdraw amount is exceed the amount of you account. You total account amount is {_balance}.");
                }
                
                _balance -= amount;
                return _balance;
            }

            throw new Exception("Account doesn't exist");
        }

        public decimal GetAccountAmount()
        {
            if (_number != 0)
            {
                return _balance;
            }

            throw new Exception("Account doesn't exist");
        }

        public void SetAccountType(AccountType accountType)
        {
            if (_number != 0)
            {
                _type = accountType;
            }
            else
            {
                throw new Exception("Account doesn't exist");
            }

        }

        public AccountType GetAccountType()
        {
            if (_number != 0)
            {
                return _type;
            }
            else
            {
                throw new Exception("Account doesn't exist");
            }
            
        }
    }
}