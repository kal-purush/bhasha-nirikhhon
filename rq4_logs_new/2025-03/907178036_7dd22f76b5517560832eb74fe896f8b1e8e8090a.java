package com.toyhe.app;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.toyhe.app.Auth.Dtos.Requests.NewInUserRequest;
import com.toyhe.app.Auth.Dtos.Requests.UserRoleRequest;
import com.toyhe.app.Auth.Services.UserManagementService;
import com.toyhe.app.Auth.Services.UserRoleService;
import com.toyhe.app.Customer.Dtos.Requests.CustomerRegisterRequest;
import com.toyhe.app.Customer.Services.CustomerService;
import com.toyhe.app.Flotte.Dtos.Boat.BoatRegisterRequest;
import com.toyhe.app.Flotte.Services.BoatService;
import com.toyhe.app.Hr.Dtos.Requests.DepartmentRegisterRequest;
import com.toyhe.app.Hr.Dtos.Requests.EmployeeRequest;
import com.toyhe.app.Hr.Dtos.Requests.PositionRequest;
import com.toyhe.app.Hr.HrService.DepartmentService;
import com.toyhe.app.Hr.HrService.EmployeeService;
import com.toyhe.app.Hr.HrService.PositionService;
import com.toyhe.app.Price.Dtos.PriceRequest;
import com.toyhe.app.Price.Service.PriceService;
import com.toyhe.app.Tickets.Dtos.ReservationRequest;
import com.toyhe.app.Tickets.Services.TicketService;
import com.toyhe.app.Trips.Dtos.Route.RouteRegisterRequest;
import com.toyhe.app.Trips.Dtos.TripRegisterRequest;
import com.toyhe.app.Trips.TripService.RouteService;
import com.toyhe.app.Trips.TripService.TripService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Component
public class DataInitializer implements CommandLineRunner {

    @Autowired
    private UserRoleService userRoleService;

    @Autowired
    private UserManagementService userManagementService;

    @Autowired
    private BoatService boatService;

    @Autowired
    private TripService  tripService;

    @Autowired
    RouteService routeService;

    @Autowired
    PriceService priceService ;

    @Autowired
    DepartmentService departmentService  ;

    @Autowired
    EmployeeService employeeService ;

    @Autowired
    PositionService positionService ;

    @Autowired
    TicketService  ticketService ;

    @Autowired
    CustomerService customerService ;





    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run(String... args) throws Exception {
        // Initialize User data only if the user table is empty
        if (userRoleService.isDatabaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/roles.json", UserRoleRequest.class, userRoleService::createUserRole);
        }

        // Initialize Product data only if the product table is empty
        if (userManagementService.isDatabaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/users.json", NewInUserRequest.class, userManagementService::createInUser);
        }

        if (boatService.isDataBaseEmpty()){
            loadAndInitializeData("src/main/resources/data/boats.json" , BoatRegisterRequest.class  , boatService::registerBoat);
        }

        if(priceService.isDataBaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/prices.json" , PriceRequest.class  , priceService::createPrice);
        }

        if(routeService.isDataBaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/route.json" , RouteRegisterRequest.class ,  routeService::registerRoute);
        }

        if (tripService.isDataBaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/trips.json" , TripRegisterRequest.class  ,  tripService::registerTrip);
        }

        if(departmentService.isDataBaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/departments.json" , DepartmentRegisterRequest.class  , departmentService::createDepartment);
        }

        if(employeeService.isDataBaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/employees.json" , EmployeeRequest.class  , employeeService::createEmployee);
        }

        if(positionService.isDataBaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/position.json" , PositionRequest.class  , positionService::createPosition);
        }

        if (ticketService.isDataBaseEmpty()) {
            loadAndInitializeData("src/main/resources/data/reservations.json" , ReservationRequest.class , ticketService::ticketReservation);
        }

//        if(customerService.isDataBaseEmpty()) {
//            loadAndInitializeData("" , CustomerRegisterRequest.class , customerService :: createCustomer);
//        }
    }

    private <T> void loadAndInitializeData(String jsonFilePath, Class<T> type, DataConsumer<T> consumer) {
        try {
            // Read JSON file into a list of objects of the specified type
            List<T> objects = objectMapper.readValue(
                    new File(jsonFilePath),
                    objectMapper.getTypeFactory().constructCollectionType(List.class, type)
            );

            // Pass each object to the provided consumer (service method)
            for (T object : objects) {
                consumer.accept(object);
            }

            System.out.println("Data initialized for: " + type.getSimpleName());
        } catch (IOException e) {
            System.err.println("Failed to load data from " + jsonFilePath + ": " + e.getMessage());
        }
    }

    @FunctionalInterface
    public interface DataConsumer<T> {
        void accept(T t);
    }
}