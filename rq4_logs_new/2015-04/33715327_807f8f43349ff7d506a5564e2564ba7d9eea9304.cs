using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WcfFynbusService.DataInterfaces
{
    public class DgInfo : Interface.EF_DataInterfaces.IDgInfo
    {
        public int ID
        {
            get; set;
        }

        public string Name
        {
            get; set;
        }

        public int CVR
        {
            get; set;
        }

        public string AltName
        {
            get; set;
        }

        public int? GuaranteeID
        {
            get; set;
        }

        public int PhoneNumber
        {
            get; set;
        }

        public byte? VehicleType
        {
            get; set;
        }

        public byte? StairMachineType
        {
            get; set;
        }

        public byte? Highchairs
        {
            get; set;
        }

        public string StreetName
        {
            get; set;
        }

        public short StreetNumber
        {
            get; set;
        }

        public short ZipCode
        {
            get; set;
        }

        public string City
        {
            get; set;
        }

        public string Municipality
        {
            get; set;
        }

        public decimal? StairMachine
        {
            get; set;
        }

        public string AdditionalInformation
        {
            get; set;
        }


        public string Registration
        {
            get; set;
        }

        public decimal WeekdaysSetup
        {
            get; set;
        }

        public decimal WeekdaysHourly
        {
            get; set;
        }

        public decimal WeekdaysDown
        {
            get; set;
        }

        public decimal WeekdaysEveningSetup
        {
            get; set;
        }

        public decimal WeekdaysEveningHourly
        {
            get; set;
        }

        public decimal WeekdaysEveningDown
        {
            get; set;
        }

        public decimal WeekendersHelligdageSetup
        {
            get; set;
        }

        public decimal WeekendersHelligdageHourly
        {
            get; set;
        }

        public decimal WeekendersHelligdageDown
        {
            get; set;
        }
    }
}