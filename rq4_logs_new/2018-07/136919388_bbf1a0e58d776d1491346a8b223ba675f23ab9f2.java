package com.kodilla.patterns.strategy.social;

import org.junit.Assert;
import org.junit.Test;

public class UserTestSuite {

    @Test
    public void testDefaultSharingStrategies() {
        //given
        User millenialsRep = new Millenials("mill");
        User yGenRep = new YGeneration("y88");
        User zGenRep = new ZGeneration("z2002");

        //when
        String millChoice = millenialsRep.sharePost();
        System.out.println(millenialsRep.getUsername() + " prefers " + millChoice);
        String y88Choice = yGenRep.sharePost();
        System.out.println(yGenRep.getUsername() + " prefers " + y88Choice);
        String z2002Choice = zGenRep.sharePost();
        System.out.println(zGenRep.getUsername() + " prefers " + z2002Choice);

        //then
        Assert.assertEquals("twitter", millChoice);
        Assert.assertEquals("facebook", y88Choice);
        Assert.assertEquals("snapchat", z2002Choice);
    }

    @Test
    public void testIndividualSharingStrategy() {
        //given
        User yGenRep = new YGeneration("y88");

        //when
        yGenRep.setSocialNetwork(new TwitterPublisher());
        String y88NewChoice = yGenRep.sharePost();
        System.out.println("\nnow " + yGenRep.getUsername() + " prefers " + y88NewChoice);

        //then
        Assert.assertEquals("twitter", y88NewChoice);
    }
}