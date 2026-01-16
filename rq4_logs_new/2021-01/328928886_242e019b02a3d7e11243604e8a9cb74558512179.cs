using System;
using System.Collections.Generic;
using System.Data.SQLite;
using Dapper;
using Martium.TravelInfo.Models;

namespace Martium.TravelInfo.Repositories
{
    public class TravelInfoRepository
    {
        public TravelInfoTextBoxModel GetExistingInfo()
        {
            using (var dbConnection = new SQLiteConnection(AppConfiguration.ConnectionString))
            {
                dbConnection.Open();

                string getExistingInfoQuery =
                    @"SELECT  
                        TI.Nation , TI.DepartureAddress , TI.FuelPrice , FSH.AdditionalKm 
                      FROM TravelInfo TI";

                TravelInfoTextBoxModel existingInfo = dbConnection.QuerySingle<TravelInfoTextBoxModel>(getExistingInfoQuery);

                return existingInfo;
            }
        }
    }
}