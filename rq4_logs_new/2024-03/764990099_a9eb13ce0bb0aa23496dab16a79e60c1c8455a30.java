package com.customs.network.fdapn.controller;

import com.customs.network.fdapn.dto.DailyAuditDTO;
import com.customs.network.fdapn.service.AuditService;
import io.micrometer.common.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;


@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private final AuditService auditService;

    @Autowired
    public AuditController(AuditService auditService) {
        this.auditService = auditService;
    }

    @GetMapping("/transactions")
    public ResponseEntity<List<DailyAuditDTO>> getUserTransactionsForWeek(@RequestParam String userId,
                                                                          @RequestParam(required = false) String period) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date endDate = calendar.getTime();
        calendar.add(Calendar.DATE, -6);
        Date startDate = calendar.getTime();

        List<DailyAuditDTO> transactions;
        if (StringUtils.isBlank(period)) {
            transactions = Collections.singletonList(auditService.getDailyAuditByUserIdAndDate(userId, endDate));
        } else {
            transactions = switch (period) {
                case "today" -> Collections.singletonList(auditService.getDailyAuditByUserIdAndDate(userId, endDate));
                case "week" -> auditService.getAuditDataForUser(userId, startDate, endDate);
                default -> throw new RuntimeException("invalid period");
            };
        }

        return new ResponseEntity<>(transactions, HttpStatus.OK);
    }

}