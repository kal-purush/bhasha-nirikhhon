package com.mate;

import com.mate.lib.Injector;
import com.mate.model.Manufacturer;
import com.mate.service.ManufacturerService;

public class Application {
    private static Injector injector = Injector.getInstance("com.mate");

    public static void main(String[] args) {
        ManufacturerService manufacturerService = (ManufacturerService) injector
                .getInstance(ManufacturerService.class);

        Manufacturer firstManufacturer = new Manufacturer("Tesla", "USA");
        Manufacturer secondManufacturer = new Manufacturer("BMW", "Germany");
        Manufacturer thirdManufacturer = new Manufacturer("Toyota", "Japan");

        manufacturerService.create(firstManufacturer);
        manufacturerService.create(secondManufacturer);
        manufacturerService.create(thirdManufacturer);

        System.out.println(manufacturerService.getAll());
        System.out.println(manufacturerService.get(1L));
        Manufacturer tempManufacturer = manufacturerService.get(1L);
        tempManufacturer.setName("Hyundai");
        tempManufacturer.setCountry("South Korea");
        manufacturerService.update(tempManufacturer);
        System.out.println(manufacturerService.get(1L));
        manufacturerService.delete(2L);
        System.out.println(manufacturerService.getAll());
    }
}