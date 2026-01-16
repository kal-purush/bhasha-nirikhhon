package liga.medical.personservice.migration.model;

import lombok.Data;
import lombok.NonNull;
import org.springframework.data.annotation.Id;

import javax.persistence.ForeignKey;

@Data
public class Address {

    @Id
    @NonNull
    private long id;

    @NonNull
    private long contactId;

    @NonNull
    private long countryId;

    @NonNull
    private String city;

    private long index;

    @NonNull
    private String street;

    @NonNull
    private String building;

    private String flat;

}