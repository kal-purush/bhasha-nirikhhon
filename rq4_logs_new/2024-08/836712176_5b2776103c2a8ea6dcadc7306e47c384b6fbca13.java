package org.example.service;

import org.example.model.*;
import org.example.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("SearchService Tests")
class SearchServiceTest {

    private CarRepository carRepository;
    private OrderRepository orderRepository;
    private UserRepository userRepository;
    private AuditRepository auditRepository;
    private SearchService searchService;

    @BeforeEach
    void setUp() {
        carRepository = mock(CarRepository.class);
        orderRepository = mock(OrderRepository.class);
        userRepository = mock(UserRepository.class);
        auditRepository = mock(AuditRepository.class);
        searchService = new SearchService(carRepository, orderRepository, userRepository, auditRepository);
    }

    @Test
    @DisplayName("Test searchCars() - Should return cars matching the given parameters")
    void testSearchCars() {
        Car car1 = new Car(1, "Toyota", "Camry", 2020, 30000.0, CarStatus.AVAILABLE);
        Car car2 = new Car(2, "Toyota", "Aygo", 2019, 20000.0, CarStatus.AVAILABLE);
        when(carRepository.findAll()).thenReturn(Arrays.asList(car1, car2));

        List<Car> result = searchService.searchCars("Toyota", null, null, null, CarStatus.AVAILABLE);

        assertEquals(1, result.size());
        assertEquals("Toyota", result.get(0).getBrand());
        assertEquals("Camry", result.get(0).getModel());
    }

    @Test
    @DisplayName("Test searchOrders() - Should return orders matching the given parameters")
    void testSearchOrders() {
        User customer = new User(1, "customer", "password", UserRole.CUSTOMER,null);
        Car car = new Car(1, "Toyota", "Camry", 2020, 30000.0, CarStatus.AVAILABLE);
        Order order1 = new Order(1, car, customer, OrderStatus.PENDING);
        Order order2 = new Order(2, car, customer, OrderStatus.COMPLETED);
        when(orderRepository.findAll()).thenReturn(Arrays.asList(order1, order2));

        List<Order> result = searchService.searchOrders(1, OrderStatus.PENDING, 1);

        assertEquals(1, result.size());
        assertEquals(OrderStatus.PENDING, result.get(0).getStatus());
    }

    @Test
    @DisplayName("Test filterUsersByRole() - Should return users matching the given parameters")
    void testSearchUsers() {
        User user1 = new User(1, "admin", "password", UserRole.ADMIN, Collections.emptyList());
        User user2 = new User(2, "manager", "password", UserRole.MANAGER, Collections.emptyList());

        // Mocking the userRepository to return the predefined list of users
        when(userRepository.findAll()).thenReturn(Arrays.asList(user1, user2));

        // Calling the method under test with the role filter
        List<User> result = searchService.searchUsers(null, null, UserRole.ADMIN, null);

        // Validating the results
        assertEquals(1, result.size(), "The result should contain exactly one user.");
        assertEquals(UserRole.ADMIN, result.get(0).getRole(), "The role of the returned user should be ADMIN.");
    }

    @Test
    @DisplayName("Test getPurchaseCount() - Should return the count of completed purchases for a user")
    void testGetPurchaseCount() {
        User customer = new User(1, "customer", "password", UserRole.CUSTOMER,null);
        Car car = new Car(1, "Toyota", "Camry", 2020, 30000.0, CarStatus.AVAILABLE);
        Order order1 = new Order(1, car, customer, OrderStatus.COMPLETED);
        Order order2 = new Order(2, car, customer, OrderStatus.COMPLETED);
        when(orderRepository.findAll()).thenReturn(Arrays.asList(order1, order2));

        int count = searchService.getPurchaseCount(1);

        assertEquals(2, count);
    }

    @Test
    @DisplayName("Test filterLogs() - Should return logs filtered by date, user, and action")
    void testFilterLogs() {
        User user = new User(1, "admin", "password", UserRole.ADMIN,null);
        LocalDateTime now = LocalDateTime.now();
        AuditLog log1 = new AuditLog(1, user, "CREATE_CAR", now.minusDays(1));
        AuditLog log2 = new AuditLog(2, user, "DELETE_CAR", now.minusDays(2));
        when(auditRepository.findAll()).thenReturn(Arrays.asList(log1, log2));

        List<AuditLog> result = searchService.filterLogs(now.minusDays(3), now, user, "CREATE_CAR");

        assertEquals(1, result.size());
        assertEquals("CREATE_CAR", result.get(0).getAction());
    }
}