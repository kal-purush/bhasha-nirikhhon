package com.codemaster.devflow.model;

import com.codemaster.devflow.model.generator.StringPrefixedSequenceIdGenerator;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;
import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Entity
@Table(name = "app_audit")
public class Audit implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "audit_seq")
    @Column(name = "audit_seq", length = 30)
    @GenericGenerator(
            name = "audit_seq",
            strategy = "com.codemaster.devflow.model.generator.StringPrefixedSequenceIdGenerator",
            parameters = {
                    @Parameter(name = StringPrefixedSequenceIdGenerator.INITIAL_PARAM, value = "10000"),
                    @Parameter(name = StringPrefixedSequenceIdGenerator.INCREMENT_PARAM, value = "1"),
                    @Parameter(name = StringPrefixedSequenceIdGenerator.VALUE_PREFIX_PARAMETER, value = "auidId"),
                    @Parameter(name = StringPrefixedSequenceIdGenerator.NUMBER_FORMAT_PARAMETER, value = "%010d") })
    private String auditId;

    @NotNull(message = "Entity Name cannot be null")
    @Size(min=2, max=50, message="Entity name must be between 2 and 50 characters")
    @Column(name = "entity_name", nullable = false, length = 50)
    private String entityName;

    @NotNull(message = "Record Id cannot be null")
    @Size(min=2, max=30, message="Record Id must be between 2 and 30 characters")
    @Column(name = "record_id", nullable = false, length = 30)
    private Long recordId;

    @NotNull(message = "Record Name cannot be null")
    @Size(min=2, max=50, message="Record Name must be between 2 and 50 characters")
    @Column(name = "record_name", nullable = false, length = 50)
    private String recordName;

    @NotNull(message = "Event Type cannot be null")
    @Size(min=2, max=50, message="Event Type must be between 2 and 50 characters")
    @Column(name = "event_type", nullable = false, length = 50)
    private String eventType;

    @NotNull(message = "Event Date cannot be null")
    @Column(name = "event_date", nullable = false)
    private LocalDateTime eventDate;

    @NotNull(message = "User Id cannot be null")
    @Size(min=2, max=30, message="User Id must be between 2 and 30 characters")
    @Column(name = "user_id", nullable = false, length = 30)
    private Long userId;

    @NotNull(message = "User Name cannot be null")
    @Size(min=2, max=50, message="User Name must be between 2 and 50 characters")
    @Column(name = "username", nullable = false, length = 50)
    private String username;

    @Lob
    @Column(name = "old_value", columnDefinition = "CLOB")
    private String oldValue;

    @Lob
    @Column(name = "new_value", columnDefinition = "CLOB")
    private String newValue;

    @NotNull(message = "Message cannot be null")
    @Size(min=2, max=500, message="Message must be between 2 and 500 characters")
    @Column(name = "message", nullable = false, length = 500)
    private String message;
}