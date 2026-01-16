package dao;

import model.Cargo;
import model.Locomotive;

import java.lang.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;


// Реализовать методы из интерфейса GenericDAO.
// Данные должны хранится в файле который расположен в дирректории "com.resources.database_test"
// (для каждой сущности создается свой файл прим. locomotive.txt).
// Пример метода getEntityById смотрите в классе LocomotiveDAO.


public class CargoDAO implements GenericDAO <Cargo> {

    String filePath = "srs/com/resources/database_test/Cargo.txt";

    @Override
    public Cargo getEntityById(Long id) {

        Cargo cargo = new Cargo();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String read = null;
            while ((read = reader.readLine()) != null) {
                String[] splitedLine = read.split(",");
                String firstStr = new String(splitedLine[0]);

                if (firstStr.equals(firstStr)) {
                    cargo.setTypeCargo(splitedLine[0]);
                    cargo.setWeightCargo(Double.parseDouble(splitedLine[1]));
                    cargo.setVolumeCargo(Double.parseDouble(splitedLine[2]));
                    cargo.setQuantityCargo(Integer.parseInt(splitedLine[3]));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cargo;
    }

    @Override
    public void saveEntity(Cargo entity) {
        try (BufferedWriter Cargo1 = new BufferedWriter(new FileWriter(filePath, true))) {

            String[] luggage = new String[]{String.valueOf(entity.getTypeCargo()),
                    String.valueOf(entity.getWeightCargo()),
                    String.valueOf(entity.getVolumeCargo()),
                    String.valueOf(entity.getQuantityCargo())};

            for (int i = 0; i < luggage.length; i++) {
                if (i != luggage.length - 1) {
                    Cargo1.write(luggage[i]);
                    Cargo1.write(",");
                } else {
                    Cargo1.write(luggage[i] + "\n");
                }
            }
        } catch (IOException e) {
            e.getStackTrace();
        }

    }

    @Override
    public void updateEntity(Cargo entity) {

        List<Cargo> cargoList = new ArrayList<>();
        try (BufferedReader cargo1 = new BufferedReader(new FileReader(filePath))) {
            String read;
            while ((read = cargo1.readLine()) != null) {
                String[] splitedLine = read.split(",");
                Cargo cargo = new Cargo();

                cargo.setTypeCargo(splitedLine[0]);
                cargo.setWeightCargo(Double.parseDouble(splitedLine[1]));
                cargo.setVolumeCargo(Double.parseDouble(splitedLine[2]));
                cargo.setQuantityCargo(Integer.parseInt(splitedLine[3]));
                cargoList.add(cargo);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Cargo cargo = cargoList.stream()
                .filter(loc -> loc.getTypeCargo().equals(entity.getTypeCargo()))
                .collect(Collectors.toList()).get(0);

        while (cargoList.contains(cargo)) {
            cargoList.remove(cargo);
        }

        cargoList.add(entity);

        try (BufferedWriter cargo1 = new BufferedWriter(new FileWriter(filePath))) {
            cargo1.write("");
        } catch (IOException e) {
            e.printStackTrace();
        }

        cargoList.forEach(this::saveEntity);
    }

    @Override
    public void removeEntity(Cargo entity) {

        List<Cargo> cargoList = new ArrayList<>();
        try (BufferedReader cargo1 = new BufferedReader(new FileReader(filePath))) {
            String read;
            while ((read = cargo1.readLine()) != null) {
                String[] splitedLine = read.split(",");
                Cargo cargo = new Cargo();


                cargo.setTypeCargo(splitedLine[0]);
                cargo.setWeightCargo(Double.parseDouble(splitedLine[1]));
                cargo.setVolumeCargo(Double.parseDouble(splitedLine[2]));
                cargo.setQuantityCargo(Integer.parseInt(splitedLine[3]));
                cargoList.add(cargo);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        Cargo cargo = cargoList.stream().filter(loc -> loc.getTypeCargo().equals(entity.getTypeCargo()))
                .collect(Collectors.toList()).get(0);

        cargoList.remove(cargo);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {
            bw.write("");
        } catch (IOException e) {
            e.printStackTrace();
        }
        cargoList.forEach(this::saveEntity);
  // }
  // }
  // class Run {
  //     public static void main(String[] args) {
  //         CargoDAO cargoDAO = new CargoDAO();
  //         Scanner scan = new Scanner(System.in);
  //         Cargo cargo = new Cargo(null,0.0,0.0,0);
  //         cargo.setTypeCargo(scan.nextLine());
  //         System.out.println(cargo.getTypeCargo());
  //         cargo.setWeightCargo(scan.nextDouble());
  //         System.out.println(cargo.getWeightCargo());

        }
}
