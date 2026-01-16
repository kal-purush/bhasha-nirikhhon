
package com.gameder.app.handlers.profiles;

import java.util.Random;

public class ProfileGenerator {

    public static String getRandomName() {
        String[] usernames = {"ferventnumber", "orcuslistener", "ableflag", "smellypapoose", "druggedoriginates",
                "croxdalemagma", "unsavouryallianz", "anxietywick", "bittenmoustache", "carillonbaseball", "polentabinding",
                "yogi-beartippy", "illogicaldancers", "minidiscnegligible", "drymitt", "absorbedknabstrup", "gazebosalisbury",
                "bolideoliver", "pebblesrigil", "finnaureole", "kokoscavaig", "riverbroth", "izzyamber", "pradacentury",
                "torontobivy", "dispenserstunning", "exclaimrobust", "walnutemmy", "discountsknock", "radialhobby",
                "accessibledagger", "dometasha", "unablearcherfish", "disguisedtruro", "smuthlisten", "glaisdalecringle",
                "retweetrude", "forgetfulenjoy", "limpqrcode", "mischiefcancer", "changeablejohannesburg",
                "mollyrhubarb", "motionlessmercy", "innocentchanges", "absorbsconcurrent", "permitglaucous", "kenziavocet",
                "ourmaxwell", "nijinskyastley", "scoonercrocodiles", "cautionvertical", "starsnavel", "fiftyformulas", "dollsgift",
                "treigmeasly", "terrineremnant", "ritecolaf", "vegetariansteamy", "rotatingdash", "slidersrocked",
                "yellerbonnie", "clinkhenmore", "warnscrew", "kyanitesize", "rhomiserly", "warblerwastebasket", "overviewamuck",
                "soundinhave", "rowlockmews", "dianafaculties", "zirconsmart", "rosyoisín", "winghamwhip", "pridejealous",
                "athletictaxi", "zealouscrap", "areastrounce", "tellingservo", "gillietiny", "shandygrimley", "spendbismarck",
                "copalmolly", "shoemakerminstral", "crackerpitter", "jokerfarrier", "shellscreeching", "vacuolespurebred",
                "footblueshift", "worthoilyritecolaf", "karachicompany"};

        Random rand = new Random();
        int index = rand.nextInt(usernames.length);
        return usernames[index];
    }

    public static String getRandomImg() {
        String url = "https://robohash.org/";
        Random rand = new Random();
        int set = rand.nextInt(10000);
        url += set;
        return url;
    }

    public static Profile getRandomProfile() {
        return new Profile(getRandomName(), getRandomImg());
    }
}