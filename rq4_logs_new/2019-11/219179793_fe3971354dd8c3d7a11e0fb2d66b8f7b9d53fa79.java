package com.lsolier.udacity.booglemaps.model.repositiry;

import static com.lsolier.udacity.booglemaps.util.Locations.ADDRESSES;

import com.lsolier.udacity.booglemaps.model.Address;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;
import java.util.stream.Collectors;

import org.springframework.stereotype.Repository;

@Repository
public class MockAddressRepository {

  public Address getRandomAddress(Double lon, Double lat) {
    Random generator = new Random();
    int randomIndex = generator.nextInt(ADDRESSES.length);
    String address = ADDRESSES[randomIndex];

    String[] addressParts = address.split(",");
    String streetAndNumber = addressParts[0];
    String cityStateAndZip = addressParts[1];

    String[] cityStateAndZipParts = cityStateAndZip.trim().split(" ");

    LinkedList<String> list =
        Arrays.stream(cityStateAndZipParts).map(String::trim)
            .collect(Collectors.toCollection(LinkedList::new));

    String zip = list.pollLast();
    String state = list.pollLast();
    String city = String.join(" ", list);

    return new Address(streetAndNumber, city, state, zip);
  }

}