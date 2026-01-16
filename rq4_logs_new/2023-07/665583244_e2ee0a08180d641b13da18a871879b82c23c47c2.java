package project.vilsoncake.BookOnlineStore.utils;

import org.springframework.stereotype.Component;
import project.vilsoncake.BookOnlineStore.constant.MessageConst;
import project.vilsoncake.BookOnlineStore.entity.AddressEntity;

import java.util.Map;

import static project.vilsoncake.BookOnlineStore.constant.MessageConst.*;

@Component
public class AddressUtils {

    public Map<String, String> validateAddress(AddressEntity address) {
        return Map.of(
                "countryError", validateCountry(address.getCountry()) ? "" : COUNTRY_SIZE_MESSAGE,
                "cityError", validateCity(address.getCity()) ? "" : CITY_SIZE_MESSAGE,
                "streetError", validateStreet(address.getStreet()) ? "" : STREET_INVALID_MESSAGE,
                "postalCodeError", validatePostalCode(address.getPostalCode()) ? "" : POSTAL_CODE_SIZE_MESSAGE
        );
    }

    public Map<String, String> addressToMap(AddressEntity address) {
        return Map.of(
                "country", address.getCountry(),
                "city", address.getCity(),
                "street", address.getStreet(),
                "postalCode", address.getPostalCode()
        );
    }

    public boolean validateCountry(String country) {
        return country.length() > 3 && country.length() < 30;
    }

    public boolean validateCity(String city) {
        return city.length() > 3 && city.length() < 30;
    }

    public boolean validateStreet(String street) {
        return (street.length() > 5 && street.length() < 64) && (street.contains("/") || street.contains("\\"));
    }

    public boolean validatePostalCode(String postalCode) {
        return postalCode.length() > 3 && postalCode.length() < 12;
    }
}