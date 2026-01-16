package com.nagarro.qathon.dashboardDataCreator;

import com.nagarro.qathon.Utility;
import com.nagarro.qathon.entity.ConversionRates;
import com.nagarro.qathon.entity.CurrentVisits;
import com.nagarro.qathon.entity.User;

public class ConversionRatesCreator {
    public static ConversionRates getConversionRates(User user){
        return new ConversionRates(
                Utility.generateIntRandomNumber(20,50),Utility.generateIntRandomNumber(500,550),
                Utility.generateIntRandomNumber(550,600),Utility.generateIntRandomNumber(600,650),
                Utility.generateIntRandomNumber(650,700),Utility.generateIntRandomNumber(700,750),
                Utility.generateIntRandomNumber(750,800),Utility.generateIntRandomNumber(850,900),
                Utility.generateIntRandomNumber(950,1000),Utility.generateIntRandomNumber(1050,1100),
                Utility.generateIntRandomNumber(1150,1200),
                user
        );
    }
}