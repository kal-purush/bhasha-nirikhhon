package pl.maks.carrental.validator;

import jakarta.validation.ValidationException;
import org.springframework.stereotype.Component;
import pl.maks.carrental.controller.productDTO.CarDTO;
import pl.maks.carrental.repository.CarRepository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.hibernate.internal.util.StringHelper.isBlank;

@Component
public class CarValidation {
    private static final Pattern ONLY_LETTERS_BRAND = Pattern.compile("^[a-zA-Z]*$");
    private static final Pattern ONLY_LETTERS_MODEL = Pattern.compile("^[a-zA-Z]*$");
    private static final Pattern ONLY_LETTERS_CAR_CLASS = Pattern.compile("^[a-zA-Z]*$");
    private static final Pattern ONLY_LETTERS_FUEL = Pattern.compile("^[a-zA-Z]*$");
    private final CarRepository carRepository;

    public CarValidation(CarRepository carRepository) {
        this.carRepository = carRepository;
    }

    public void carValidation(CarDTO carDTO) {
        List<String> violations = new ArrayList<>();

        if (isBlank(carDTO.getBrand())) {
            violations.add("Brand is blank");
        }
        if (!ONLY_LETTERS_BRAND.matcher(carDTO.getBrand()).matches()) {
            violations.add(String.format("Brand can contain only letters: ", carDTO.getBrand()));
        }
        if (isBlank(carDTO.getModel())) {
            violations.add("Model is blank");
        }
        if (!ONLY_LETTERS_MODEL.matcher(carDTO.getModel()).matches()) {
            violations.add(String.format("Model can contain only letters: " + carDTO.getModel()));
        }

        if (isBlank(carDTO.getCarClass())) {
            violations.add("CarClass is blank");
        }
        if (!ONLY_LETTERS_CAR_CLASS.matcher(carDTO.getCarClass()).matches()) {
            violations.add(String.format("CarClass can contain only letters: ", carDTO.getCarClass()));
        }

        if (isBlank(carDTO.getFuel())) {
            violations.add("Fuel is blank");
        }
        if (!ONLY_LETTERS_FUEL.matcher(carDTO.getFuel()).matches()) {
            violations.add(String.format(String.format("Fuel can contain only letters: ", carDTO.getFuel())));
        }

        if (carDTO.getPricePerDay() == null || carDTO.getPricePerDay().compareTo(BigDecimal.ZERO) <= 0) {
            violations.add("PricePerDay must not be null and greater than zero");
        }

        if (carDTO.getParking() == null || carDTO.getParking().getId() == null) {
            violations.add("Parking id is null");
        }

        if (!violations.isEmpty()) {
            String violationMessage = String.join(", ", violations);
            throw new ValidationException("Provided car details are invalid: " + violationMessage);
        }
    }
}