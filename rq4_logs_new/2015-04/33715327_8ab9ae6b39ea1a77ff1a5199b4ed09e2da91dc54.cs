using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.ServiceModel;

namespace WcfFynbusService.Interfaces
{
    [ServiceContract]
    public interface IOffersform_SingleVehicle
    {
        [OperationContract]
        int NewBasicInformation(string token, string name, int cvr, string nameAlt);

        [OperationContract]
        int NewWagone(string token, int? wagonGuaranteeId, string registrationLetters, int registrationNumbers, 
            int phoneNumber, byte vehicleType, byte? stairMachine, byte? highchairs, int? homeWagon, int owneBy);

        [OperationContract]
        int NewHomeWagon(string token, string streetName, short streetNumber, short zipCode, string city, string municipality);

        [OperationContract]
        int NewPrice(string token, int WeekdaysID, int WeekdaysEveningID, int WeekendersHelligdageID, decimal StairMachine);

        [OperationContract]
        int NewPricePlan(string token, decimal setUpFee, decimal hourlyRate, decimal hourlydDoenTime);

        [OperationContract]
        int NewOffersform_SingleVehicle(string token, int basicInformationID, int wagonDetailsID, int priceID, string addInfo);
    }
}