package skillfactory.DreamTeam.globus.it.dto.bank;

import java.io.ObjectInputFilter.Status;
import java.math.BigDecimal;
import java.time.LocalDate;

import org.springframework.boot.actuate.endpoint.OperationType;
import org.springframework.format.annotation.DateTimeFormat;

import lombok.Data;

@Data
public class OperationFilter {
    private Long senderBankId;
    private Long receiverBankId;
    private String inn;
    private OperationType type;
    private String category;
    private Status status;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    private LocalDate fromDate;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
    private LocalDate toDate;

    private BigDecimal minAmount;
    private BigDecimal maxAmount;
}