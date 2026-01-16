package com.dufuna.berlin.tolulopebabatunde.lekki;

import com.dufuna.berlin.tolulopebabatunde.lekki.model.LekkiProperty;
import com.dufuna.berlin.tolulopebabatunde.lekki.service.LekkiPropertyService;
import com.dufuna.berlin.tolulopebabatunde.lekki.service.MockLekkiPropertyService;

public class MockLekkiPropertyApp {
    public static void main(String[] args) {
        LekkiProperty prop = new LekkiProperty();
        LekkiPropertyService service = new MockLekkiPropertyService();

        service.saveProperty(prop);
        service.getProperty();
    }


}