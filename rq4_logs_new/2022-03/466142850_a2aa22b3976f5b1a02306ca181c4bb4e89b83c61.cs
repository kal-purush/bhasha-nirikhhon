using System;
using System.Text;

namespace Lesson_02
{
    public enum AccountType
    {
        Undefined,
        Current,    // Текущий, расчетный
        Credit,     // Кредитный
        Deposit     // Депозит
    }

    public class AccountTask01
    {
        // Создать класс счет в банке с закрытыми полями: номер счета, баланс, тип банковского счета (использовать перечислимый тип).
        // Предусмотреть методы для доступа к данным – заполнения и чтения.
        // Создать объект класса, заполнить его поля и вывести информацию об объекте класса на печать


        private UInt64 _number;             // Номер счета 
        private Decimal _balance;           // Баланс счета
        private AccountType _type;          // Тип счета

 
        public UInt64 CreateAccount(UInt64 accNumber)
        {
            _number = accNumber;
            return _number;
        }

        public UInt64 GetAccountNumber()
        {
            return _number;
        }

        public void SetBalance(Decimal amount)
        {
            if (_number != 0)
            {
                _balance = amount;
            }
            else
            {
                throw new Exception("Account doesn't exist");
            }
        }

        public Decimal GetBalance()
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