package skillfactory.DreamTeam.globus.it.dto.bank;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BankAccountDTO {
    private final Long id;
    private final Long holderId;
    private final Long bankId;
    private final BigDecimal balance;
    private final String accountNumber;
    private final String title;
    private final boolean active;
    private final boolean deleted;
    private final LocalDateTime createDateTime;
    private final LocalDateTime updateDateTime;
}