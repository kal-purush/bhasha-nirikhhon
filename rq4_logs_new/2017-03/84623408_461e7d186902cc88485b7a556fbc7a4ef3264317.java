package com.anca.exchange_rate.api;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Path;
import org.simpleframework.xml.Root;

import java.util.List;

/**
 * Created by ishy159@gmail.com on 11-Mar-17.
 */

@Root(name = "query", strict = false)
public class ERResponse {

    @ElementList(name = "rate", inline = true)
    @Path("results")
    private List<ExchangeRate> lstExchangeRate;

    public List<ExchangeRate> getLstExchangeRate() {
        return lstExchangeRate;
    }

    public void setLstExchangeRate(List<ExchangeRate> lstExchangeRate) {
        this.lstExchangeRate = lstExchangeRate;
    }
}