package com.example.projectmrsisa.validators;

import com.example.projectmrsisa.dto.*;
import com.example.projectmrsisa.model.User;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Validator {

    private String[] fishingEquipment = {"Hooks", "Lines", "Floats", "Rods", "Reels", "Baits", "Lures", "Spears", "Nets", "Gaffs", "Traps"};
    private String[] navigationEquipment = {"Gyro Compass", "Radar", "Autopilot", "Echo sounder", "Magnetic Compass", "ARPA", "GPS Receiver", "Navigation lamps"};

    public boolean validAddress(String country, String city, String street) {
        if (country.equals("") || country == null || !country.matches("([A-Z]{1})([a-z]+)([^0-9]*)$")) {
            return false;
        }
        if (city.equals("") || city == null || !city.matches("([A-Z]{1})([a-z]+)([^0-9]*)$")) {
            return false;
        }
        return !street.equals("") && street != null;
    }

    public boolean validateRetreatData(RetreatDTO retreatDTO) {
        if (retreatDTO.getName() == null || retreatDTO.getName().equals("")){
            return false;
        }
        if (retreatDTO.getDescription() == null || retreatDTO.getDescription().equals("")){
            return false;
        }
        if (retreatDTO.getNumOfRooms() <= 0) {
            return false;
        }
        for (String as: retreatDTO.getAdditionalServices()) {
            if (as.equals("") || as.length() > 14) return false;
        }
        for (String rc: retreatDTO.getRulesOfConduct()) {
            if (rc.equals("")) return false;
            if (!rc.equals("Inside smoking disallowed") && !rc.equals("Pet friendly") && !rc.equals("Parties disallowed") && !rc.equals("Furniture braking is charged extra")) return false;
        }
        for (String pic: retreatDTO.getPictures()) {
            if (pic.equals("")) return false;
            if (!pic.endsWith(".png") && !pic.endsWith(".jpg") && !pic.endsWith(".jpeg")) return false;
        }
        return retreatDTO.getNumOfBeds() > 0;
    }

    public boolean validServiceAvailability(ServiceAvailabilityDTO serviceAvailabilityDTO) {
        if (!validDates(serviceAvailabilityDTO.getDateFrom(), serviceAvailabilityDTO.getDateTo(), serviceAvailabilityDTO.getTimeFrom(), serviceAvailabilityDTO.getTimeTo())) return false;
        return true;
    }

    public boolean validDates(Date dateFrom, Date dateTo, String timeFrom, String timeTo) {
        Date today = new Date();
        if (dateFrom == null || dateTo == null || timeFrom == null || timeTo == null) return false;
        if (dateFrom.compareTo(today) < 0) return false;
        if (dateTo.compareTo(today) < 0 ) return false;
        if (dateFrom.compareTo(dateTo) > 0) return false;
        if (dateFrom.compareTo(dateTo) == 0) {
            return Integer.parseInt(timeFrom) * 60 + Integer.parseInt(timeFrom)
                    < Integer.parseInt(timeTo) * 60 + Integer.parseInt(timeTo);
        }
        return true;
    }

    public boolean validAction(ActionDTO actionDTO) {
        if (!validDates(actionDTO.getDateFrom(), actionDTO.getDateTo(), actionDTO.getTimeFrom(), actionDTO.getTimeTo())) return false;
        System.out.println(actionDTO.getMaxNumOfPeople());
        if (actionDTO.getMaxNumOfPeople() <= 0) return false;
        for (String as: actionDTO.getAdditionalServices()) {
            if (as.equals("") || as.length() > 14) return false;
        }
        return !(actionDTO.getPrice() <= 0);
    }

    public boolean validateShipData(ShipDTO shipDTO) {
        if (shipDTO.getName() == null || shipDTO.getName().equals("")){
            return false;
        }
        if (shipDTO.getDescription() == null || shipDTO.getDescription().equals("")){
            return false;
        }
        if (shipDTO.getType() == null || shipDTO.getType().equals("")) {
            return false;
        }
        if (shipDTO.getLength() <= 5) {
            return false;
        }
        if (shipDTO.getEngineNum() == null || shipDTO.getEngineNum().equals("")) {
            return false;
        }
        if (shipDTO.getEnginePower() <= 40) {
            return false;
        }
        if (shipDTO.getPrice() <= 0) return false;
        if (shipDTO.getMaxSpeed() == null || shipDTO.getMaxSpeed().matches("[1-9][0-9]* 'km/h'")) {
            return false;
        }
        if (shipDTO.getCapacity() <= 0) {
            return false;
        }
        for (String pic: shipDTO.getPictures()) {
            if (pic.equals("")) return false;
            if (!pic.endsWith(".png") && !pic.endsWith(".jpg") && !pic.endsWith(".jpeg")) return false;
        }
        for (String rc: shipDTO.getRulesOfConduct()) {
            if (rc.equals("")) return false;
            if (!rc.equals("Smoking disallowed") && !rc.equals("Pet friendly") && !rc.equals("Parties disallowed") && !rc.equals("Drinking disallowed") && !rc.equals("Weapons disallowed")) return false;
        }
        ArrayList<String> fishEq = new ArrayList<>(Arrays.asList(fishingEquipment));
        for (String fe: shipDTO.getFishingEquipment()) {
            if (fe.equals("")) return false;
            if (!fishEq.contains(fe)) return false;
        }
        ArrayList<String> navEq = new ArrayList<>(Arrays.asList(navigationEquipment));
        for (String ne: shipDTO.getNavigationEquipment()) {
            if (ne.equals("")) return false;
            if (!navEq.contains(ne)) return false;
        }
        return shipDTO.getReservationCancellationConditions() != null;
    }

    public boolean validateAdventure(AdventureDTO adventureDTO) {
        if (adventureDTO.getName().length() < 5 || adventureDTO.getName() == null) {
            return false;
        }
        if (adventureDTO.getDescription().length() < 5 || adventureDTO.getDescription() == null) {
            return false;
        }
        if (adventureDTO.getPrice() < 5) {
            return false;
        }
        if (adventureDTO.getMaxNumOfPeople() < 1) {
            return false;
        }
        if (adventureDTO.getReservationCancellationConditions().length() < 5 || adventureDTO.getReservationCancellationConditions() == null) {
            return false;
        }
        if (adventureDTO.getInstructorBiography().length() < 5 || adventureDTO.getInstructorBiography() == null) {
            return false;
        }
        if (!validAddress(adventureDTO.getCountry(), adventureDTO.getCity(), adventureDTO.getStreet())){
            return false;
        }
        return true;
    }

    public boolean validReservationDTO(ReservationDTO reservationDTO) {
        if (reservationDTO.getClientEmail() == null || !reservationDTO.getClientEmail().matches("^\\w+([\\.-]?\\w+)*@\\w+([\\.-]?\\w+)*(\\.\\w{2,3})+$")) {
            return false;
        }
        if (!validDates(reservationDTO.getFromDate(), reservationDTO.getToDate(), reservationDTO.getFromTime(), reservationDTO.getToTime()))
            return false;
        for (String as : reservationDTO.getAdditionalServices()) {
            if (as.equals("") || as.length() > 14) return false;
        }
        return reservationDTO.getNumOfPeople() > 0;
    }

    public boolean validUser(UserDTO userDTO) {
        if (!validUserData(userDTO)) return false;
        return userDTO.getRegistrationType().equals("client") || userDTO.getRegistrationType().equals("privilegedUser");
    }

    public boolean validAdmin(UserDTO adminDTO) {
        return validUserData(adminDTO);
    }

    public boolean validUserData(UserDTO userDTO) {
        if (userDTO.getEmail() == null || !userDTO.getEmail().matches("^\\w+([\\.-]?\\w+)*@\\w+([\\.-]?\\w+)*(\\.\\w{2,3})+$")) {
            return false;
        }
        if (userDTO.getPassword() == null || userDTO.getConfirmPassword() == null || !userDTO.getPassword().equals(userDTO.getConfirmPassword())) {
            return false;
        }
        if (!validChangedUserInfo(userDTO)) return false;
        if (userDTO.getPhoneNumber() == null || userDTO.getPhoneNumber().equals("") || !userDTO.getPhoneNumber().matches("^[+]?(\\d{1,2})?[\\s.-]?\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}$")) {
            return false;
        }
        return true;
    }

    public boolean validChangedUserInfo(UserDTO userDTO){
        if (userDTO.getName() == null ||userDTO.getName().equals("") || !userDTO.getName().matches("([A-Z]{1})([a-z]+)([^0-9]*)$")) {
            return false;
        }
        if (userDTO.getSurname() == null ||userDTO.getSurname().equals("") || !userDTO.getSurname().matches("([A-Z]{1})([a-z]+)([^0-9]*)$")) {
            return false;
        }
        return userDTO.getPhoneNumber() != null && !userDTO.getPhoneNumber().equals("") && userDTO.getPhoneNumber().matches("^[+]?(\\d{1,2})?[\\s.-]?\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}$");
    }

    public boolean validRegistrationData(UserDTO userDTO) {
        if (userDTO.getRegistrationType().equals("privilegedUser")) {
            if (userDTO.getRegistrationExplanation() == null || userDTO.getRegistrationExplanation().equals("")) return false;
            return userDTO.getPrivilegedUserType().equals("retreatOwner") || userDTO.getPrivilegedUserType().equals("shipOwner") || userDTO.getPrivilegedUserType().equals("fishingInstructor");
        }
        return true;
    }
}