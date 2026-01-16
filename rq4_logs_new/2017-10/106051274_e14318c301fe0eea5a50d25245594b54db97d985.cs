namespace BankCase
{
    /// <summary>
    /// Represents a bank account
    /// </summary>
    public class Account
    {
        /// <summary>
        /// Creates a new bank account
        /// </summary>
        /// <param name="name">The account name</param>
        /// <param name="owner">The account's owner</param>
        /// <param name="balance">The initial balance of the account</param>
        public Account(string name, Person owner, Money balance)
        {
            this.Name = name;
            this.Owner = owner;
            this.Balance = balance;
        }

        /// <summary>
        /// The name of the account
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// A reference to the owner of the account
        /// </summary>
        public Person Owner { get; }

        /// <summary>
        /// The current balance of the account
        /// </summary>
        public Money Balance { get; set; }
    }
}