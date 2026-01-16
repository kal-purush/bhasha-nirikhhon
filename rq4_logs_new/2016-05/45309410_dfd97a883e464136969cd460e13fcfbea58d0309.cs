namespace PhoneBookNamespace
{
    /// <summary>
    /// interface of phone book
    /// </summary>
    interface IPhoneBook
    {
        /// <summary>
        /// adds given record to the database
        /// </summary>
        /// <param name="newRecord"></param>
        void Add(Record newRecord);

        /// <summary>
        /// prints name of person with given telephone number to the console
        /// </summary>
        /// <param name="number"></param>
        void FindByNumber(string number);

        /// <summary>
        /// prints telephone numbers of persons with given name
        /// </summary>
        /// <param name="name"></param>
        void FindByName(string name);

        /// <summary>
        /// prints all records of data base to console
        /// </summary>
        void Print();
    }
}