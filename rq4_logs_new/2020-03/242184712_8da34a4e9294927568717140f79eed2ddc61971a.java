package by.epam.dao;

import by.epam.entities.CarPart;

import java.io.*;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class CarPartDao {
    private ObjectOutputStream outputStream;

    public void create(CarPart carPart) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/CarPart.txt", true))) {
            writer.write(carPart.toStringFile());
            writer.newLine();
        } catch (IOException ignored) {
        }
    }

    public ArrayList<CarPart> readAll() {
        try (BufferedReader reader = new BufferedReader(new FileReader("data/CarPart.txt"))) {
            ArrayList<CarPart> carParts = new ArrayList<>();
            String buffer;
            while ((buffer = reader.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(buffer, "-");
                int id = Integer.parseInt(st.nextToken());
                String name = st.nextToken();
                String description = st.nextToken();
                String CarId = st.nextToken();
                carParts.add(new CarPart(id, name, description, CarId));
            }
            return carParts;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void deleteByName(String name, ArrayList<CarPart> carParts) {
        // TODO: 19.03.2020 create delete method

    }

    public void writeAll(ArrayList<CarPart> carParts) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File("data", "CarPart.txt")))) {
            for (CarPart carPart : carParts) {
                String temp = "-";
                writer.write(Integer.toString(carPart.getId()));
                writer.write(temp);
                writer.write(carPart.getName());
                writer.write(temp);
                writer.write(carPart.getDescription());
                writer.write(temp);
                writer.write(carPart.getCarId());
                writer.write("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getMaxId() {
        ArrayList<CarPart> carParts = readAll();
        int result = 0;
        for (CarPart carPart : carParts) {
            if (carPart.getId() > result) {
                result = carPart.getId();
            }
        }
        return result;
    }

    public void Update(int id, CarPart newCarPart) {
        ArrayList<CarPart> carParts = readAll();
        for (CarPart carPart : carParts) {
            if (id == carPart.getId()) {
                carPart.setName(newCarPart.getName());
                carPart.setDescription(newCarPart.getDescription());
                carPart.setCarId(newCarPart.getCarId());
                break;
            }
        }
        writeAll(carParts);
    }

    public CarPart readCarPart(int id) {
        ArrayList<CarPart> carParts = readAll();
        for (CarPart carPart : carParts) {
            if (id == carPart.getId()) {
                return carPart;
            }
        }
        throw new IllegalArgumentException(String.format("Id %d не найден", id));
    }

    public void delete(int id) {
        ArrayList<CarPart> carParts = readAll();
        for (CarPart carPart : carParts) {
            if (id == carPart.getId()) {
                carParts.remove(carPart);
                writeAll(carParts);
                return;
            }
        }
        throw new IllegalArgumentException(String.format("Id %d не найден", id));
    }
}