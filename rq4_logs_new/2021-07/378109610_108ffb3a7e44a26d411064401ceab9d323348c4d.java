public class Address {
    private String country;
    private String city;

    //сделаем констуктор для создания объекта адреса призывника
    public Address(String countryConscript, String cityConscript) {
        this.country = countryConscript;
        this.city = cityConscript;
    }


    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "Address{" +
                "country='" + country + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}