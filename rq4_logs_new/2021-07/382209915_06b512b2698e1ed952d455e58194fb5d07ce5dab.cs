using System;
using System.Collections.Generic;
using System.Text;

namespace Address_Book
{
    class ContactPersonComparer : IComparer<AddressBookSystem>  
    {
        //Constants 
        public enum sortBy
        {
            firstName,
            city,
            state,
            zip
        }
        public sortBy compareByFields;

        //Compare values of addressBook object x and y
        public int Compare(AddressBookSystem x, AddressBookSystem y)
        {
            switch (compareByFields)
            {
                case sortBy.firstName:
                    return x.firstName.CompareTo(y.firstName);
                case sortBy.city:
                    return x.city.CompareTo(y.city);
                case sortBy.state:
                    return x.state.CompareTo(y.state);
                case sortBy.zip:
                    return x.zip.CompareTo(y.zip);
                default: break;

            }
            //If given Invalid Option
            return x.firstName.CompareTo(y.firstName);

        }
    }
}