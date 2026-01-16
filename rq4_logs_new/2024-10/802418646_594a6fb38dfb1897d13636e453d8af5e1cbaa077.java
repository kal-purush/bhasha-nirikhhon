package com.petid.domain.hospital.manager;

import com.petid.domain.exception.HospitalHourNotFoundException;
import com.petid.domain.hospital.model.HospitalHour;
import com.petid.domain.hospital.repository.HospitalHourRepository;
import com.petid.domain.hospital.type.DayType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.util.Set;
import java.util.TreeSet;

@Component
@RequiredArgsConstructor
public class HospitalHourManager {

    private final HospitalHourRepository hospitalHourRepository;

    public HospitalHour getByHospitalIdAndDay(
            long hospitalId,
            DayType day
    ) {
        return hospitalHourRepository.findByHospitalIdAndDay(hospitalId, day)
                .orElseThrow(() -> new HospitalHourNotFoundException(hospitalId, day));
    }

    public Set<LocalTime> getAvailableTimes(
            HospitalHour hourData
    ) {
        Set<LocalTime> availableTimes = new TreeSet<>();

        int orderInterval = 30;
        LocalTime currentTime = hourData.openingTime();
        LocalTime lastAvailableTime = hourData.closingTime().minusMinutes(orderInterval);
        LocalTime lastAvailableTimeBeforeBreak = hourData.breakingTime().minusMinutes(orderInterval);
        LocalTime breakEndTime = hourData.breakingTime().plusMinutes(hourData.breakingUnit());

        while (!currentTime.isAfter(lastAvailableTime)) {
            if (currentTime.isAfter(lastAvailableTimeBeforeBreak) && currentTime.isBefore(breakEndTime)) {
                currentTime = breakEndTime;
            } else {
                availableTimes.add(currentTime);
                currentTime = currentTime.plusMinutes(orderInterval);
            }
        }

        return availableTimes;
    }
}