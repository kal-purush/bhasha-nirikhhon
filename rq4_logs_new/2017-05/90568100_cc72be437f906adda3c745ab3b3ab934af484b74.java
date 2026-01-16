package com.gameder.app.handlers.profiles;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.TreeSet;

public class ProfileSetTest {
    @Test
    public void ProfileSetIsFilled() {
        ProfilesHandler p = new ProfilesHandler();

        p.generateProfiles();

        TreeSet<Profile> ts = p.getProfilesTreeset();
        Iterator ite = ts.iterator();

        while(ite.hasNext()) {
            Object pro = ite.next();
            System.out.println(pro.toString());
        }

        Assert.assertFalse(ts.isEmpty());
    }

    @Test
    public void ProfileSetIsInOrder() {
        ProfilesHandler p = new ProfilesHandler();

        p.generateProfiles();

        TreeSet<Profile> ts = p.getProfilesTreeset();

        Assert.assertTrue(ts.first().getPreference() < ts.last().getPreference());
    }

}