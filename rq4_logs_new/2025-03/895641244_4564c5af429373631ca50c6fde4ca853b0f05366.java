package repositories.impl;

import cars.Car;
import cars.PerformanceCar;
import cars.ShowCar;
import exceptions.CarAlreadyExistsException;
import exceptions.CarNotFoundException;
import repositories.CarsRepository;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CarsRepositoryFileImpl implements CarsRepository {

    private static final Map<String, Car> CARS = new LinkedHashMap<>();
    public static final String FILE_PATH = "src/main/resources/cars.txt";

    public CarsRepositoryFileImpl() {
        var file = new File(FILE_PATH);
        try {
            if (!file.createNewFile()) {
                loadCars(file);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadCars(File file) throws IOException {
        try (var reader = new BufferedReader(new FileReader(file))) {
            reader.lines().map(this::mapLineToCar).forEach(car -> CARS.put(car.getId(), car));
        }
    }

    private Car mapLineToCar(String line) {
        String[] tokens = line.split("\\|");
        var carType = tokens[0];
        Car car;
        if (carType.equals("PerformanceCar")) {
            var performanceCar = new PerformanceCar();
            List<String> addOns = Arrays.asList(tokens[9].split(","));
            performanceCar.setAddOns(addOns);
            car = performanceCar;
        } else if (carType.equals("ShowCar")) {
            var showCar = new ShowCar();
            var stars = Integer.parseInt(tokens[9]);
            showCar.setStars(stars);
            car = showCar;
        } else {
            throw new IllegalArgumentException("Неизвестный тип автомобиля: " + carType);
        }
        car.setId(tokens[1]);
        car.setBrand(tokens[2]);
        car.setModel(tokens[3]);
        car.setYear(Integer.parseInt(tokens[4]));
        car.setPower(Integer.parseInt(tokens[5]));
        car.setAcceleration(Integer.parseInt(tokens[6]));
        car.setPendant(Integer.parseInt(tokens[7]));
        car.setDurability(Integer.parseInt(tokens[8]));
        return car;
    }

    private String mapCarToString(Car car) {
        String[] values = {
                car.getType(),
                car.getId(),
                car.getBrand(),
                car.getModel(),
                car.getYear().toString(),
                car.getPower().toString(),
                car.getAcceleration().toString(),
                car.getPendant().toString(),
                car.getDurability().toString()
        };
        return String.join("|", values);
    }

    @Override
    public void create(Car car) {
        if (CARS.containsKey(car.getId())) {
            throw new CarAlreadyExistsException();
        }
        CARS.put(car.getId(), car);
        saveToFile();
    }

    private void saveToFile() {
        try (
                var writer = Files.newBufferedWriter(Paths.get(FILE_PATH), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        ) {
            for (var car : CARS.values()) {
                var line = mapCarToString(car);
                writer.write(line + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Car findById(String id) {
        if (!CARS.containsKey(id)) {
            throw new CarNotFoundException();
        }
        return CARS.get(id);
    }

    @Override
    public List<Car> findAll() {
        return CARS.values().stream().toList();
    }

    @Override
    public void update(Car car) {
        CARS.put(car.getId(), car);
        saveToFile();
    }

    @Override
    public void deleteById(String id) {
        if (!CARS.containsKey(id)) {
            throw new CarNotFoundException();
        }
        CARS.remove(id);
        saveToFile();
    }

    @Override
    public void deleteAll() {
        CARS.clear();
        saveToFile();
    }
}