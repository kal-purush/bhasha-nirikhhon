package com.codingblocks.codequiz.api;


import com.codingblocks.codequiz.dummy_utils.DummyTip;
import com.google.gson.Gson;
import io.undertow.server.RoutingHandler;
import io.undertow.server.HttpServerExchange;
/**
 * Created by piyush0 on 28/12/16.
 */
public class TipHandler extends RoutingHandler {
    public TipHandler(){
        super();
        this.get("/todayTip", this::GET_todayTip);

    }

    private void GET_todayTip(HttpServerExchange exchange) throws Exception{
        String tip = DummyTip.getTips().get(DummyTip.getTips().size()-1);
        Gson gson = new Gson();
        exchange.getResponseSender().send(gson.toJson(tip));
    }


}