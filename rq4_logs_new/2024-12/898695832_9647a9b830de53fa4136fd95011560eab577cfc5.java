package ru.clevertec.carshowroom;

import ru.clevertec.carshowroom.dto.car.AddCarDTO;
import ru.clevertec.carshowroom.dto.car.OutputCarDTO;
import ru.clevertec.carshowroom.dto.car.UpdateCarDTO;
import ru.clevertec.carshowroom.dto.carshowroom.AddCarShowroomDTO;
import ru.clevertec.carshowroom.dto.carshowroom.UpdateCarShowroomDTO;
import ru.clevertec.carshowroom.dto.category.AddCategoryDTO;
import ru.clevertec.carshowroom.dto.category.UpdateCategoryDTO;
import ru.clevertec.carshowroom.dto.client.AddClientDTO;
import ru.clevertec.carshowroom.dto.client.OutputClientDTO;
import ru.clevertec.carshowroom.dto.client.UpdateClientDTO;
import ru.clevertec.carshowroom.dto.review.AddReviewDTO;
import ru.clevertec.carshowroom.dto.review.UpdateReviewDTO;
import ru.clevertec.carshowroom.exception.CarNotFoundException;
import ru.clevertec.carshowroom.exception.CarShowroomNotFoundException;
import ru.clevertec.carshowroom.exception.CategoryNotFoundException;
import ru.clevertec.carshowroom.exception.ClientNotFoundException;
import ru.clevertec.carshowroom.exception.ReviewNotFoundException;
import ru.clevertec.carshowroom.mapper.CarMapper;
import ru.clevertec.carshowroom.mapper.CarMapperImpl;
import ru.clevertec.carshowroom.mapper.CarShowroomMapperImpl;
import ru.clevertec.carshowroom.mapper.CategoryMapperImpl;
import ru.clevertec.carshowroom.mapper.ClientMapper;
import ru.clevertec.carshowroom.mapper.ClientMapperImpl;
import ru.clevertec.carshowroom.mapper.ReviewMapperImpl;
import ru.clevertec.carshowroom.repository.car.impl.CarRepositoryImpl;
import ru.clevertec.carshowroom.repository.carshowroom.impl.CarShowroomRepositoryImpl;
import ru.clevertec.carshowroom.repository.category.impl.CategoryRepositoryImpl;
import ru.clevertec.carshowroom.repository.client.impl.ClientRepositoryImpl;
import ru.clevertec.carshowroom.repository.review.impl.ReviewRepositoryImpl;
import ru.clevertec.carshowroom.service.car.CarService;
import ru.clevertec.carshowroom.service.car.impl.CarServiceImpl;
import ru.clevertec.carshowroom.service.carshowroom.CarShowroomService;
import ru.clevertec.carshowroom.service.carshowroom.impl.CarShowroomServiceImpl;
import ru.clevertec.carshowroom.service.category.CategoryService;
import ru.clevertec.carshowroom.service.category.impl.CategoryServiceImpl;
import ru.clevertec.carshowroom.service.client.ClientService;
import ru.clevertec.carshowroom.service.client.impl.ClientServiceImpl;
import ru.clevertec.carshowroom.service.review.ReviewService;
import ru.clevertec.carshowroom.service.review.impl.ReviewServiceImpl;
import ru.clevertec.carshowroom.util.TestData;

public class Main {

    public static void main(String[] args) {
        //testCarCRUDOperations();
        //testCarShowroomCRUDOperations();
        //testCategoryCRUDOperations();
//        testClientCRUDOperations();
//        testReviewCRUDOperations();

    }

    private static void testCarCRUDOperations() {
        CarService carService = new CarServiceImpl(new CarRepositoryImpl(), new CarMapperImpl(), new CarShowroomMapperImpl());

        try {
            AddCarDTO addCarDTO = TestData.createAddCarDTO();
            System.out.println(carService.addCar(addCarDTO));

            carService.findAll().forEach(System.out::println);

            System.out.println(carService.findById(2L));

            UpdateCarDTO updateCarDTO = TestData.createUpdateCarDTO();
            System.out.println(carService.update(updateCarDTO));

            System.out.println(carService.deleteById(5L));
        } catch (CarNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    private static void testCarShowroomCRUDOperations() {
        CarShowroomService carShowroomService =
                new CarShowroomServiceImpl(new CarShowroomRepositoryImpl(), new CarShowroomMapperImpl());

        try {
            AddCarShowroomDTO addCarShowroomDTO = TestData.createAddCarShowroomDTO();
            System.out.println(carShowroomService.addCarShowroom(addCarShowroomDTO));

            carShowroomService.findAll().forEach(System.out::println);

            System.out.println(carShowroomService.findById(2L));

            UpdateCarShowroomDTO updateCarShowroomDTO = TestData.createUpdateCarShowroomDTO();
            System.out.println(carShowroomService.update(updateCarShowroomDTO));

            System.out.println(carShowroomService.deleteById(3L));
        } catch (CarShowroomNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    private static void testCategoryCRUDOperations() {
        CategoryService categoryService =
                new CategoryServiceImpl(new CategoryRepositoryImpl(), new CategoryMapperImpl());

        try {
            AddCategoryDTO addCategoryDTO = TestData.createAddCategoryDTO();
            System.out.println(categoryService.addCategory(addCategoryDTO));

            categoryService.findAll().forEach(System.out::println);

            System.out.println(categoryService.findById(4L));

            UpdateCategoryDTO updateCategoryDTO = TestData.createUpdateCategoryDTO();
            System.out.println(categoryService.update(updateCategoryDTO));

            System.out.println(categoryService.deleteById(4L));
        } catch (CategoryNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    private static void testClientCRUDOperations() {
        ClientService clientService =
                new ClientServiceImpl(new ClientRepositoryImpl(), new ClientMapperImpl(), new CarMapperImpl());

        try {
            AddClientDTO addClientDTO = TestData.createAddClientDTO();
            System.out.println(clientService.addClient(addClientDTO));

            clientService.findAll().forEach(System.out::println);

            System.out.println(clientService.findById(7L));

            UpdateClientDTO updateClientDTO = TestData.createUpdateClientDTO();
            System.out.println(clientService.update(updateClientDTO));

            System.out.println(clientService.deleteById(7L));
        } catch (ClientNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    private static void testReviewCRUDOperations() {
        ReviewService reviewService =
                new ReviewServiceImpl(new ReviewRepositoryImpl(), new ReviewMapperImpl());
        CarMapper carMapper = new CarMapperImpl();
        ClientMapper clientMapper = new ClientMapperImpl();
        CarService carService = new CarServiceImpl(new CarRepositoryImpl(), new CarMapperImpl(), new CarShowroomMapperImpl());
        ClientService clientService = new ClientServiceImpl(new ClientRepositoryImpl(), new ClientMapperImpl(), new CarMapperImpl());

        try {
            OutputCarDTO outputCarDTO = carService.findById(2L);
            UpdateCarDTO updateCarDTO = carMapper.toUpdateCarDTO(outputCarDTO);

            OutputClientDTO outputClientDTO = clientService.findById(2L);
            UpdateClientDTO updateClientDTO = clientMapper.toUpdateClientDTO(outputClientDTO);


            AddReviewDTO addReviewDTO = TestData.createAddReviewDTO(updateCarDTO, updateClientDTO);
            System.out.println(reviewService.addReview(addReviewDTO));

            reviewService.findAll().forEach(System.out::println);

            System.out.println(reviewService.findById(4L));

            OutputCarDTO outputCarDTO1 = carService.findById(1L);
            UpdateCarDTO updateCarDTO1 = carMapper.toUpdateCarDTO(outputCarDTO1);

            OutputClientDTO outputClientDTO1 = clientService.findById(2L);
            UpdateClientDTO updateClientDTO1 = clientMapper.toUpdateClientDTO(outputClientDTO1);

            UpdateReviewDTO updateReviewDTO = TestData.createUpdateReviewDTO(updateCarDTO1, updateClientDTO1);
            System.out.println(reviewService.update(updateReviewDTO));
            System.out.println(reviewService.deleteById(3L));
        } catch (ReviewNotFoundException ex) {
            ex.printStackTrace();
        }
    }
}