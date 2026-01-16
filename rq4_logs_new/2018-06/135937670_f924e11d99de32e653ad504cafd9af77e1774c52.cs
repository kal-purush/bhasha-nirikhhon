using System;
using System.Collections.Generic;

namespace SiMarket
{
    class ListActions
    {
        private List<SharePacket> list;
        public List<SharePacket> List
        {
            get
            {
                return list;
            }
        }

        public void SortUp()
        {
            List.Sort((SP1, SP2) => SP1.Price.CompareTo(SP2.Price));

        }

        public void SortDown()
        {
            List.Sort((SP1, SP2) => SP2.Price.CompareTo(SP1.Price));

        }

        public void SortCompany()
        {
            List.Sort((SP1, SP2) => SP1.CompanyName.CompareTo(SP2.CompanyName));

        }

        public void Initialize()
        {
            list = new List<SharePacket>();
        }
        
        public int FindNumber(string companyName)
        {
            int number = 0;
            foreach (SharePacket packet in List)
            {
                if (List[number].CompanyName == companyName)
                {
                    return number;
                }
                number++;
            }
            return -1;
        }

        public int FindNumber(string companyName, decimal price, int quantity)
        {
            int number = 0;
            foreach (SharePacket packet in List)
            {
                if (List[number].CompanyName == companyName && List[number].Price == price && List[number].CurrentQuantity == quantity)
                {
                    return number;
                }
                number++;
            }
            return -1;
        }

        public void PrintList()
        {
            Console.WriteLine();
            foreach (SharePacket cmp in Market.Current.CompaniesList.List)
            {
                int i = FindNumber(cmp.CompanyName);
                if (i != -1)
                {
                    Console.WriteLine("{0}: {1} shares, best price: {2}$", cmp.CompanyName, GetAmountOfShares(cmp.CompanyName), List[i].Price);
                }
            }
        }

        public void PrintList(string companyName)
        {
            Console.WriteLine();
            int number = 0;
            List<int> numbers = new List<int>();
            foreach (SharePacket packet in List)
            {
                if (List[number].CompanyName == companyName)
                {
                    numbers.Add(number);
                }
                number++;
            }
            foreach (int i in numbers)
            {
                Console.WriteLine("{0} shares, each for {1}$", List[i].CurrentQuantity, List[i].Price);
            }
        }

        public int GetAmountOfShares(string companyName)
        {
            int amount = 0;
            int i = 0;
            foreach (SharePacket packet in List)
            {
                if (List[i].CompanyName == companyName)
                {
                    amount += List[i].CurrentQuantity;
                }
                i++;
            }
            return amount;
        }

        public void RemoveEmptyEntries()
        {
            int i = 0;
            while (i < List.Count)
            {
                if (List[i].CurrentQuantity == 0)
                {
                    List.RemoveAt(i);
                    i--;
                }
                i++;
            }
        }

        public List<SharePacket> GetPackets(string companyName)
        {
            List<SharePacket> list = new List<SharePacket>();
            foreach (var packet in List)
            {
                if (packet.CompanyName == companyName)
                {
                    SharePacket packet1 = new SharePacket
                    {
                        Age = packet.Age,
                        Quantity = packet.Quantity,
                        CurrentQuantity = packet.CurrentQuantity,
                        Price = packet.Price,
                        Owner = packet.Owner,
                        CompanyName = packet.CompanyName
                    };
                    list.Add(packet1);
                    packet.CurrentQuantity = 0;
                }
            }
            RemoveEmptyEntries(); 
            return list;
        }

        public List<string> ReturnAllCompanies()
        {
            List<string> list = new List<string>();
            foreach (var packet in List)
            {
                if (!list.Contains(packet.CompanyName))
                {                    
                    list.Add(packet.CompanyName);
                }
            }
            return list;
        }
    }
}