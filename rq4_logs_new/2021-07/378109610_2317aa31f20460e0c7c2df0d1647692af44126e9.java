package com.artpro.service;


import com.artpro.model.GroundTransport;
import com.artpro.model.Transport;

public class TransportAction {
    public void calculation(Transport transport, double time) {
        double way = transport.getMaxSpeed() * time;
//    GroundTransport groundTransport = (GroundTransport) transport;
        GroundTransport groundTransport = (GroundTransport) transport; //посмотреть лекцию
        double v = groundTransport.getFuelСonsumption() * way / 100;
        System.out.println("за время" + time + "автомобиль" + transport.getBrand() + v);
    }
}