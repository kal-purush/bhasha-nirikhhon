package bloodbank.bloodbankservice.core.dtos;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Value;

import java.io.Serializable;

/**
 * DTO for Blood Bank Entity class.
 *
 * @author Muhammad Bilal Khan
 * @since 1.0.0
 * @version 1.0.0
 * @see bloodbank.bloodbankservice.core.entities.BloodBank
 */
@Value
public class BloodBankDto implements Serializable {
    Long id;

    @NotBlank(message = "Blood Bank Name is a required field and should not be blank.")
    String bloodBankName;

    @NotBlank(message = "Address is a required field and should not be blank.")
    String address;

    @NotBlank(message = "City is a required field and should not be blank.")
    String city;

    @Pattern(message = "Phone number does not conform with the regex provided.", regexp = "^(\\+\\d{1,2}\\s?)?\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}$")
    @NotBlank(message = "Phone Number is a required field and should not be blank.")
    String phoneNumber;

    @Pattern(message = "Website does not conform with the regex provided.", regexp = "https?:\\/\\/(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9()@:%_\\+.~#?&//=]*)")
    @NotBlank(message = "Website is a required field and should nto be blank.")
    String website;

    @Email(message = "Email that was provided is not of valid format.")
    @NotBlank(message = "Email is a required field and should not be blank.")
    String email;
}