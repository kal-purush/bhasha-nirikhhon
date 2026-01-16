package com.vc.model;

public class City
{
    private String airportCode;

    private String country;

    private String city;

    public String getAirportCode ()
    {
        return airportCode;
    }

    public void setAirportCode (String airportCode)
    {
        this.airportCode = airportCode;
    }

    public String getCountry ()
    {
        return country;
    }

    public void setCountry (String country)
    {
        this.country = country;
    }

    public String getCity ()
    {
        return city;
    }

    public void setCity (String city)
    {
        this.city = city;
    }

    @Override
    public String toString()
    {
        return "ClassPojo [airportCode = "+airportCode+", country = "+country+", city = "+city+"]";
    }
}