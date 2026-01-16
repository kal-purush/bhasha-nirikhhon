package com.levchenko.hw23;

import com.levchenko.hw23.model.Device;
import com.levchenko.hw23.model.Factory;
import com.levchenko.hw23.model.FactoryStats;
import com.levchenko.hw23.service.Utils;

import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        final Factory factoryFirst = new Factory(1, "QWE");
        final Factory factorySecond = new Factory(2, "ASD");
        final Factory factoryThird = new Factory(3, "ZXC");
        final Factory factoryFourth = new Factory(4, "DFG");
        Utils utils = new Utils();
        utils.addFactory(factoryFirst);
        utils.addFactory(factorySecond);
        utils.addFactory(factoryThird);
        utils.addFactory(factoryFourth);
        utils.addDevice(utils.createDevice());
        utils.addDevice(utils.createDevice());
        utils.addDevice(utils.createDevice());
        utils.addDevice(utils.createDevice());
        utils.addDevice(utils.createDevice());
        utils.addDevice(utils.createDevice());
        utils.addDevice(utils.createDevice());
        utils.addDevice(utils.createDevice());
        utils.addDevice(utils.createDevice());

        Device device = utils.getDeviceAndFactory(1);
        System.out.println(device.toString());

        utils.updateDevice(1, "test", "test", 0, new Date(new java.util.Date().getTime()), 1);

        utils.deleteDevice(6);
        utils.deleteDevice(2);

        List<Device> list = utils.sameFactoryDevices(1);
        for (Device device1 : list) {
            System.out.println(device1);
        }

        List<FactoryStats> listStats = utils.getFactoryStats();
        for (FactoryStats factoryStats : listStats) {
            System.out.println(factoryStats.toString());
        }
    }
}