using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BuyWithOffers.App_Code
{
    public class Utility
    {
        public readonly int iphone6Discount = 5;
        public readonly int birthdayDiscount = 10;
        public readonly int seniorCitizenDiscount = 15;
        public readonly int highValueOrderCitizen = 20;
        private double total = 0;
        private double grandTotal = 0;
        private bool isBirthday = false;
        public bool IsBirthday
        {
            get { return isBirthday; }
            set { isBirthday = value; }
        }
        private bool isSeniorCitizen = false;
        public bool IsSeniorCitizen
        {
            get { return isSeniorCitizen; }
            set { isSeniorCitizen = value; }
        }

        //Iphone6 members and properties
        private int iphone6 = 300;
        private int numberOfiphone6 = 0;
        public void Setiphone6price(int totalnumberOfiphone6)
        {
            numberOfiphone6 = totalnumberOfiphone6;

        }
        public double Getiphone6price()
        {
            return iphone6 * numberOfiphone6;
        }

        //Iphone5 members and properties
        private int iphone5 = 200;
        private int numberOfiphone5 = 0;
        public void Setiphone5price(int totalnumberOfiphone5)
        {
            numberOfiphone5 = totalnumberOfiphone5;
        }

        public double Getiphone5price()
        {
            return iphone5 * numberOfiphone5;
        }

        //Ipad members and properties
        private double ipad5 = 500;
        private int numberOfipad5 = 0;
        public void Setipadprice(int totalnumberOfipad5)
        {
            numberOfipad5 = totalnumberOfipad5;
        }
        public double Getipadprice()
        {
            return ipad5 * numberOfipad5;
        }

        //macbook air members and properties
        private double macBookAir = 1000;
        private int numberOfmacBookAir = 0;
        public void SetmacBookAirprice(int totalnumberOfmacBookAir)
        {
            numberOfmacBookAir = totalnumberOfmacBookAir;
        }
        public double GetmacBookAirprice()
        {
            return macBookAir * numberOfmacBookAir;
        }

        //macbook pro members and properties
        private double macBookpro = 2000;
        private int numberOfmacBookpro = 0;
        public void SetmacBookproprice(int totalnumberOfmacBookpro)
        {
            numberOfmacBookpro = totalnumberOfmacBookpro;
        }
        public double GetmacmacBookproprice()
        {
            return macBookpro * numberOfmacBookpro;
        }

        //calculates single discount scheme
        public void SetSingleDiscountScheme(bool isBuyingMutipleiphone6, bool isBirthday, bool isSeniorCitizen)
        {
            grandTotal = total;
            if (grandTotal > 3000)
                grandTotal = total - (total * highValueOrderCitizen / 100);
            else if (isSeniorCitizen)
                grandTotal = total - (total * seniorCitizenDiscount / 100);
            else if (isBirthday)
                grandTotal = total - (total * birthdayDiscount / 100);
            else if (isBuyingMutipleiphone6)
                grandTotal = total - (total * iphone6Discount / 100);


        }

        //calculates multiple discount scheme
        public void SetDoubleDiscountScheme(bool isBuyingMutipleiphone6, bool isBirthday, bool isSeniorCitizen)
        {
            int discount = 0;
            grandTotal = total;
            if (grandTotal > 3000)
                discount = discount + highValueOrderCitizen;

            if (isSeniorCitizen)
                discount = discount + seniorCitizenDiscount;

            if (isBirthday)
                discount = discount + birthdayDiscount;

            if (isBuyingMutipleiphone6)
                discount = discount + iphone6Discount;

            grandTotal = total - (total * discount / 100);
        }

        //sets total amount without discounts
        public void SetTotalAmount()
        {
            total = Getiphone6price() + Getiphone5price() + Getipadprice() + GetmacBookAirprice() + GetmacmacBookproprice();

        }


        public double GetGrandTotal(bool isSinglDiscount)
        {
            SetTotalAmount();
            bool isMultipleiphone6 = false;
            if (numberOfiphone6 > 1)
            {
                isMultipleiphone6 = true;
            }

            if (isSinglDiscount)
                SetSingleDiscountScheme(isMultipleiphone6, IsBirthday, IsSeniorCitizen);
            else
                SetDoubleDiscountScheme(isMultipleiphone6, IsBirthday, IsSeniorCitizen);

            return grandTotal;
        }
    }
}