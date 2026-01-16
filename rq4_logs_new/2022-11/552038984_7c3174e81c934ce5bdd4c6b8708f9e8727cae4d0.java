package com.tricentis.demowebshop.config;

import org.aeonbits.owner.Config;
@Config.Sources({
        "classpath:credentials.properties"

})

public interface DataConfig extends Config {
    @Key("user.email")
    String getUserEmail();

    @Key("sample.email")
    String getUndefinedEmail();

    @Key("sample.password")
    String getUndefinedPassword();

    @Key("deliver.country")
    String getDeliverCountry();

    @Key("deliver.state")
    String getDeliverState();

    @Key("deliver.zip")
    String getDeliverZip();

    @Key("deliver.address")
    String getDeliverAddress();

    @Key("deliver.city")
    String getDeliverCity();

    @Key("deliver.phone")
    String getDeliverPhone();

    @Key("user.password")
    String getUserPassword();

}