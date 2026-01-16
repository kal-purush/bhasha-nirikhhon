package com.interlink.hotel.Apartment;

public enum AccommodationType implements Criterion {
    SGL,DBL;

    @Override
    public boolean validate(ApartmentEntity apartment) {
        return this == apartment.getAccommodationType();
    }
}