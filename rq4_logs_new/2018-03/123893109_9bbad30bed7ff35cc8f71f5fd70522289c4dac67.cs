using System;
using System.Collections.Generic;
using F18I4DABH2Gr24Lib;
using Console = System.Console;

namespace F18I4DABH2Gr24
{
    class Program
    {
        static void Main(string[] args)
        {
            var person1 = new Person()
            {
                FirstName = "Rasmus",
                LastName = "Lund",
                Email = "Raslund85@gmail.com",
                Context = "A friend, not drunk, not at all, nope!"
            };
            var person2 = new Person()
            {
                FirstName = "Søren",
                MiddleName = "EtEllerAndet",
                LastName = "Ryge",
                Email = "Ryger@gmail.com",
                Context = "A Smoker"
            };
            var personIndex = new List<Person>()
            {
                person1,
                person2
            };


            var telephoneNumber1 = new TelephoneNumber()
            {
                Number = "+45 28 99 02 58",
                Use = "Private"
            };
            var telephoneNumber2 = new TelephoneNumber()
            {
                Number = "+45 29 99 02 58",
                Use = "NotPrivate"
            };
            var telephoneNumbers = new List<TelephoneNumber>
            {
                telephoneNumber1,
                telephoneNumber2
            };


            var telephoneCompany = new TelephoneCompany()
            {
                Company = "Telia"
            };


            var addressType1 = new AddressType()
            {
                Type = "Primary"
            };
            var addressType2 = new AddressType()
            {
                Type = "Secondary"
            };


            var addressTypes = new List<AddressType>
            {
                addressType1,
                addressType2
            };


            var address1 = new Address()
            {
                HouseNumber = "1A",
                StreetName = "Gertrudvej"
            };
            var address2 = new Address()
            {
                HouseNumber = "5A",
                StreetName = "IngenAlkoholTilRasmus"
            };
            var addresses = new List<Address>
            {
                address1,
                address2
            };


            var zipCode = new ZipCode()
            {
                Zip = "8000"
            };


            var countryCode = new CountryCode()
            {
                Code = "DK"
            };
            var countryCodes = new List<CountryCode>()
            {
                countryCode
            };


            var city = new City
            {
                Name = "Aarhus"
            };

            // Connecting the items
            // Could use different persons with different value, but it's a lot of work..! need ze factory
            person1.AddressTypes = addressTypes;
            person1.AlternativeAddresses = addresses;
            person1.PrimaryAddress = address1;
            person1.TelephoneNumbers = telephoneNumbers;
            person2.AddressTypes = addressTypes;
            person2.AlternativeAddresses = addresses;
            person2.PrimaryAddress = address2;
            person2.TelephoneNumbers = telephoneNumbers;

            telephoneCompany.TelephoneNumbers = telephoneNumbers;

            address1.AddressTypes = addressTypes;
            address1.Persons = personIndex;
            address1.City = city;
            address2.AddressTypes = addressTypes;
            address2.Persons = personIndex;
            address2.City = city;

            addressType1.Persons = personIndex;
            addressType1.Address = address1;
            addressType2.Persons = personIndex;
            addressType2.Address = address2;

            zipCode.CityName = city;
            zipCode.CountryCode = countryCode;

            city.ZipCode = zipCode;

            foreach (var person in personIndex)
            {
                PrintPerson(person);
            }

            foreach (var address in addresses)
            {
                PrintAddress(address);
            }

            PrintTelephones(telephoneCompany);
        }

        private static void PrintTelephones(TelephoneCompany telephoneCompany)
        {
            Console.WriteLine($"Printing telephone numbers under {telephoneCompany.Company}");
            Console.WriteLine("Numbers: ");
            foreach (var telephoneNumber in telephoneCompany.TelephoneNumbers)
            {
                Console.WriteLine($"   {telephoneNumber.Number}");
            }

        }

        public static string getName(Person person)
        {
            return String.Format($"{person.FirstName} {person.MiddleName} {person.LastName}");
        }

        private static void PrintAddress(Address address)
        {
            Console.WriteLine("Printing Addresses and associated People");
            Console.WriteLine($"Street name: {address.StreetName} HouseNumber: {address.HouseNumber}");
            foreach (var person in address.Persons)
            {
                Console.WriteLine($"Name of person; {getName(person)} Context: {person.AddressTypes.Find(at => at.Address == address).Type}");
            }

            Console.WriteLine();
            Console.WriteLine("----------------------------------------------------");
            Console.WriteLine();
            Console.WriteLine();
        }

        private static void PrintPerson(Person person)
        {
            Console.WriteLine("Printing Person");
            Console.WriteLine(person.MiddleName != null
                ? $"Name: {person.FirstName} {person.MiddleName} {person.LastName}"
                : $"Name: {person.FirstName} {person.LastName}");
            Console.WriteLine($"Context: {person.Context}");

            Console.WriteLine($"Email: {person.Email}");
            Console.WriteLine("Telephone Number:");
            foreach (var telephoneNumber in person.TelephoneNumbers)
            {
                Console.WriteLine($"   {telephoneNumber.Number} Use {telephoneNumber.Use}" );
            }
            Console.WriteLine();

            Console.WriteLine("Addresses:");
            foreach (var address in person.AlternativeAddresses)
            {
                Console.WriteLine($"   Address type: {address.AddressTypes.Find(a => a.Address == address).Type}");
                Console.WriteLine($"   Street name: {address.StreetName} HouseNumber: {address.HouseNumber}");
                Console.WriteLine($"   City: {address.City.Name} Zip-code: {address.City.ZipCode.Zip} Country code: {address.City.ZipCode.CountryCode.Code}");
                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine("----------------------------------------------------");
            Console.WriteLine();
            Console.WriteLine();
        }
    }
}