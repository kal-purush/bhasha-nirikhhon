package za.ac.cput.repository;

import za.ac.cput.domain.RentTruck;

import java.util.ArrayList;
import java.util.List;
/**
 * RentTruckRepository.java
 * This is the repository program
 * @aurthor Zukhanye Anele Mene (219404275)
 * Date: 23 March 2024
 */
public class RentTruckRepository implements IRentTruckRepository {

    private static IRentTruckRepository rentTruckRepository  = null;

    List<RentTruck> rentedTruckList;

    private RentTruckRepository() {

        rentedTruckList = new ArrayList<RentTruck>();
    }

    public static IRentTruckRepository getRentTruckRepository() {
        if (rentTruckRepository == null) {
            rentTruckRepository = new RentTruckRepository();
        }
        return rentTruckRepository;
    }

    @Override
    public List<RentTruck> getAll() {
        return rentedTruckList;
    }

    @Override
    public RentTruck create(RentTruck rentedTruck) {
        boolean success = rentedTruckList.add(rentedTruck);
        if (success)
            return rentedTruck;
        return null;
    }

    @Override
    public RentTruck read(String id) {

        int rentalID = Integer.parseInt(id);
        for (RentTruck rentedTruck : rentedTruckList) {
             if (rentedTruck.getRentalID() == rentalID )
                return rentedTruck;
        }
        return null;
    }

    @Override
    public RentTruck update(RentTruck rentedTruck) {
         int rentalID = rentedTruck.getRentalID();
        if (delete(String.valueOf(rentalID))) {
            if (rentedTruckList.add(rentedTruck))
                return rentedTruck;
            else
                return null;
        }
        return null;
    }

    @Override
    public boolean delete(String id) {
        RentTruck rentalToDelete = read(id);
        if (rentalToDelete == null)
            return false;
        return (rentedTruckList.remove(rentalToDelete));
    }
}

