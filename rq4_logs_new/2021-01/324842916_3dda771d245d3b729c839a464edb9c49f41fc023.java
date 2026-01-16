package viewModels;

import static helpers.MathHelpers.round2;

public class AccountStatus {
    private final Float alreadyPaid;
    private final Float pending;
    private final Double latestBalance;
    private final Double adjustedAvailable;

    AccountStatus(Float alreadyPaid, Float pending, Double lastBalance) {
        this.alreadyPaid = alreadyPaid;
        this.pending = pending;
        this.latestBalance = lastBalance;
        this.adjustedAvailable = round2(this.latestBalance - this.pending);
    }

    public Float getAlreadyPaid() {
        return round2(alreadyPaid);
    }

    public Float getPending() {
        return round2(pending);
    }

    public Double getLatestBalance() {
        return round2(latestBalance);
    }

    public Double getAdjustedAvailable() {
        return adjustedAvailable;
    }
}