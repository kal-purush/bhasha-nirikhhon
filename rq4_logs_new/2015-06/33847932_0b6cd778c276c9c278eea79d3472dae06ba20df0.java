package com.gracenotes.controller;

import com.gracenotes.model.*;
import com.gracenotes.util.PasswordEncryptionHelper;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by adam on 6/7/15.
 */
@RestController
@RequestMapping("/report")
public class ReportController {
    @Value("${key}")
    private String masterKey;

    @Autowired
    MongoOperations mongoOperation;

    @RequestMapping(value = "/monthlyByDay", method= RequestMethod.GET)
    @ResponseBody
    public ResponseJSON monthlyByDay(@RequestHeader("Authorization") String key) {
        ResponseJSON response = new ResponseJSON();
        MetaJSON meta = new MetaJSON();
        MonthlyByDayReport monthlyByDayReport = new MonthlyByDayReport();
        // decrypt the key
        try {
            String decryptedKey = PasswordEncryptionHelper.decrypt(key);
            List<String> credentials = Arrays.asList(decryptedKey.split(":"));
            String email = credentials.get(0);
            String hashedPassword = credentials.get(1);
            Query searchUserQuery = new Query(Criteria.where("email").is(email));
            User savedUser = mongoOperation.findOne(searchUserQuery, User.class);
            boolean passed = false;
            if (null != savedUser) {
                if(savedUser.getPassword().equals(hashedPassword)) { passed = true; }
            }
            if(key.equals(masterKey)) { passed = true; }
            if(passed) {
                List<CourseRequest> courseRequests = mongoOperation.findAll(CourseRequest.class);
                List<Registration> registrations = mongoOperation.findAll(Registration.class);
                // get the current month
                LocalDate currentDate = new LocalDate();
                LocalDate monthBeginningDate = currentDate.withDayOfMonth(1);
                LocalDate monthEndDate = monthBeginningDate.plusMonths(1);
                LocalDate iteratorDate = monthBeginningDate;
                ArrayList<String> daysInMonth = new ArrayList<>();
                ArrayList<Integer> courseRequestsByDay = new ArrayList<>();
                ArrayList<Integer> registrationsByDay = new ArrayList<>();
                while(iteratorDate.getMonthOfYear() < monthEndDate.getMonthOfYear()) {
                    daysInMonth.add(String.valueOf(iteratorDate.getDayOfMonth()));
                    if(iteratorDate.getDayOfMonth() <= currentDate.getDayOfMonth()) {
                        courseRequestsByDay.add(0);
                        for(CourseRequest courseRequest : courseRequests) {
                            LocalDate localDate = DateTime.parse(courseRequest.getCreatedAt()).toLocalDate();
                            if(localDate.getDayOfMonth() == iteratorDate.getDayOfMonth()) {
                                courseRequestsByDay.set(courseRequestsByDay.size()-1, courseRequestsByDay.get(courseRequestsByDay.size()-1)+1);
                            }
                        }
                        registrationsByDay.add(0);
                        for(Registration registration : registrations) {
                            LocalDate localDate = DateTime.parse(registration.getCreatedAt()).toLocalDate();
                            if(localDate.getDayOfMonth() == iteratorDate.getDayOfMonth()) {
                                registrationsByDay.set(registrationsByDay.size()-1, registrationsByDay.get(registrationsByDay.size()-1)+1);
                            }
                        }
                    } else {
                        courseRequestsByDay.add(null);
                        registrationsByDay.add(null);
                    }
                    iteratorDate = iteratorDate.plusDays(1);
                }
                monthlyByDayReport.setCourseRequestsByDay(courseRequestsByDay);
                monthlyByDayReport.setDaysInMonth(daysInMonth);
                monthlyByDayReport.setRegistrationsByDay(registrationsByDay);
                monthlyByDayReport.setSubtitle(currentDate.toString("MMM YYYY"));
                meta.setStatus(1);
                meta.setStatusText("SUCCESS");
            } else {
                meta.setStatus(2);
                meta.setStatusText("Invalid key");
            }
        } catch (Exception e) {
            meta.setStatus(3);
            meta.setStatusText(e.getMessage());
            e.printStackTrace();
        }
        response.setData(monthlyByDayReport);
        response.setMeta(meta);
        return response;
    }

}