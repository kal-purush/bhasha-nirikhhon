using MyTestOnVending.VendingMachine;
using System.Collections.Generic;

namespace MyTestOnVending.Constants
{
    public static class VendingMachineConstants
    {
        public static List<ProductDetails> ProductList = new List<ProductDetails>{
            new ProductDetails{SerialNumber=1, ProductName="Cola", Price=1.00m}
            ,new ProductDetails{SerialNumber=2,ProductName="Chips", Price=0.50m}
            ,new ProductDetails{SerialNumber=3,ProductName="Candy", Price=0.65m}
            };
        public static List<Coins> ValidCoinList = new List<Coins>{
            new Coins{SerialNumber=7, TypeOfCoin="Nickels", CoinValue=0.05m}
            ,new Coins{SerialNumber=8,TypeOfCoin="Dimes", CoinValue=0.10m}
            ,new Coins{SerialNumber=9,TypeOfCoin="Quarters", CoinValue=0.25m}
            };
    }
}