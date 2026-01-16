package project.service;

import org.springframework.stereotype.Service;
import project.responseentities.subentities.DriverListEntry;
import project.responseentities.subentities.RiderListEntry;

import java.util.List;

/**
 * Created by olafurorn on 10/22/15.
 */
@Service
public class DriverRiderService
{
    public DriverRiderService()
    {

    }

    public void createDriverListEntry(
            int phoneNumber,
            int lowPrice,
            int highPrice,
            int numberOfPeople,
            String place,
            String carDescription,
            String message,
            long startTimeTimestamp,
            long endTimeTimestamp
            //String accessToken, TODO: check the accessToken)
    )
    {
        // TODO:
    }

    public void createRiderListEntry(
            int phoneNumber,
            int price,
            int numberOfPeople,
            String location,
            String destination,
            String message,
            long pickUpTimeTimestamp
            // String accessToken, TODO: check the accessToken
    )
    {
        // TODO:
    }

    public List<DriverListEntry> getDriverListEntry()
    {
        return null; // TODO:
    }

    public List<RiderListEntry> getRiderListEntry()
    {
        return null; // TODO:
    }
}