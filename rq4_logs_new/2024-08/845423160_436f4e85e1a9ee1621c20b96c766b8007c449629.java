package fiveguys.innout.service;

import fiveguys.innout.entity.User;
import fiveguys.innout.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;

@Service
public class AgeGroupService {

    @Autowired
    private UserRepository userRepository;

    public String calculateAgeGroup(Long userId) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException("User not found"));

        Date birthDate = user.getBirthDate();
        if (birthDate == null) {
            throw new RuntimeException("User birth date is not set");
        }

        Calendar birthCalendar = Calendar.getInstance();
        birthCalendar.setTime(birthDate);

        int birthYear = birthCalendar.get(Calendar.YEAR);

        Calendar currentCalendar = Calendar.getInstance();
        int currentYear = currentCalendar.get(Calendar.YEAR);

        int age = currentYear - birthYear;

        if (age < 20) {
            return "10대";
        } else if (age < 30) {
            return "20대";
        } else if (age < 40) {
            return "30대";
        } else if (age < 50) {
            return "40대";
        } else if (age < 60) {
            return "50대";
        } else if (age < 70) {
            return "60대";
        } else {
            return "70대 이상";
        }
    }
}