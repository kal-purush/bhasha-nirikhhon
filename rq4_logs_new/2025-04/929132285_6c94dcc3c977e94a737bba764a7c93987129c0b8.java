package nl.optifit.backendservice.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum SessionStatus {
    NEW("New"),
    COMPLETED("Completed"),
    OVERDUE("Overdue");

    private final String displayName;

    @JsonCreator
    public static SessionStatus fromString(String value) {
        try {
            return SessionStatus.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            // Handle unknown values if needed
            return null;
        }
    }

    @JsonValue
    public String toValue() {
        return this.name();
    }
}