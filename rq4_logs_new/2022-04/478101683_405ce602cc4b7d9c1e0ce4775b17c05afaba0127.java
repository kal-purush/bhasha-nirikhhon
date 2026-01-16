package com.nagarro.qathon.dashboardDataCreator;

import com.nagarro.qathon.Utility;
import com.nagarro.qathon.entity.CampaignClicks;
import com.nagarro.qathon.entity.ConversionRates;
import com.nagarro.qathon.entity.User;

import java.util.ArrayList;
import java.util.List;

public class CampaignClicksCreator {

    public static List<CampaignClicks> getCampaignClicks(User user) {
        List<CampaignClicks> campaignClicks = new ArrayList<CampaignClicks>();

        CampaignClicks newDelhiCampaignClicks = new CampaignClicks();
        newDelhiCampaignClicks.setCampaignOneClicks(Utility.generateIntRandomNumber(50,100));
        newDelhiCampaignClicks.setCampaignTwoClicks(Utility.generateIntRandomNumber(10,40));
        newDelhiCampaignClicks.setCampaignThreeClicks(Utility.generateIntRandomNumber(30,60));
        newDelhiCampaignClicks.setCityName("New Delhi");
        newDelhiCampaignClicks.setUser(user);
        campaignClicks.add(newDelhiCampaignClicks);

        CampaignClicks mumbaiCampaignClicks = new CampaignClicks();
        mumbaiCampaignClicks.setCampaignOneClicks(Utility.generateIntRandomNumber(30,60));
        mumbaiCampaignClicks.setCampaignTwoClicks(Utility.generateIntRandomNumber(10,40));
        mumbaiCampaignClicks.setCampaignThreeClicks(Utility.generateIntRandomNumber(60,90));
        mumbaiCampaignClicks.setCityName("Mumbai");
        mumbaiCampaignClicks.setUser(user);
        campaignClicks.add(mumbaiCampaignClicks);

        CampaignClicks bengaluruCampaignClicks = new CampaignClicks();
        bengaluruCampaignClicks.setCampaignOneClicks(Utility.generateIntRandomNumber(10,40));
        bengaluruCampaignClicks.setCampaignTwoClicks(Utility.generateIntRandomNumber(30,60));
        bengaluruCampaignClicks.setCampaignThreeClicks(Utility.generateIntRandomNumber(70,90));
        bengaluruCampaignClicks.setCityName("Bengaluru");
        bengaluruCampaignClicks.setUser(user);
        campaignClicks.add(bengaluruCampaignClicks);


        CampaignClicks hyderabadCampaignClicks = new CampaignClicks();
        hyderabadCampaignClicks.setCampaignOneClicks(Utility.generateIntRandomNumber(30,60));
        hyderabadCampaignClicks.setCampaignTwoClicks(Utility.generateIntRandomNumber(70,90));
        hyderabadCampaignClicks.setCampaignThreeClicks(Utility.generateIntRandomNumber(10,30));
        hyderabadCampaignClicks.setCityName("Hyderabad");
        hyderabadCampaignClicks.setUser(user);
        campaignClicks.add(hyderabadCampaignClicks);

        CampaignClicks chennaiCampaignClicks = new CampaignClicks();
        chennaiCampaignClicks.setCampaignOneClicks(Utility.generateIntRandomNumber(100,110));
        chennaiCampaignClicks.setCampaignTwoClicks(Utility.generateIntRandomNumber(10,40));
        chennaiCampaignClicks.setCampaignThreeClicks(Utility.generateIntRandomNumber(30,60));
        chennaiCampaignClicks.setCityName("Chennai");
        chennaiCampaignClicks.setUser(user);
        campaignClicks.add(chennaiCampaignClicks);

        CampaignClicks puneCampaignClicks = new CampaignClicks();
        puneCampaignClicks.setCampaignOneClicks(Utility.generateIntRandomNumber(10,40));
        puneCampaignClicks.setCampaignTwoClicks(Utility.generateIntRandomNumber(60,100));
        puneCampaignClicks.setCampaignThreeClicks(Utility.generateIntRandomNumber(10,30));
        puneCampaignClicks.setCityName("Pune");
        puneCampaignClicks.setUser(user);
        campaignClicks.add(puneCampaignClicks);

        return campaignClicks;

    }
}