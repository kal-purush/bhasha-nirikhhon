package com.reservibe.infra.adapter.restaurant;

import com.reservibe.domain.entity.restaurant.Address;
import com.reservibe.domain.entity.restaurant.OpeningHours;
import com.reservibe.domain.entity.restaurant.Restaurant;
import com.reservibe.domain.entity.table.Table;
import com.reservibe.domain.enums.retaurant.Cuisine;
import com.reservibe.domain.enums.table.TableStatus;
import com.reservibe.infra.model.restaurant.RestaurantModel;
import com.reservibe.infra.repository.restaurant.RestaurantRepository;
import com.reservibe.infra.repository.table.TableModelRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class RestaurantRegisterAdapterTest {

    @Mock
    private RestaurantRepository restaurantRepository;

    @Mock
    private TableModelRepository tableModelRepository;

    private RegisterRestaurantAdapter adapter;

    AutoCloseable mock;

    @BeforeEach
    void setup(){
        mock = MockitoAnnotations.openMocks(this);
        adapter = new RegisterRestaurantAdapter(restaurantRepository, tableModelRepository);
    };

    @AfterEach
    void tearDown() throws Exception {
        mock.close();
    }

    @Test
    void shouldSaveRestaurant(){
        var id = UUID.randomUUID();
        var restaurant = createRestaurant(id);

        when(restaurantRepository.save(any(RestaurantModel.class)))
                .thenAnswer(i -> i.getArgument(0)); //essa chamada acessa um método e verifica dentro dele as informações (deve receber um professional e retornar esse professional)

        var restaurantCreated = adapter.registerRestaurant(restaurant);

        assertThat(restaurantCreated)
                .isInstanceOf(Restaurant.class)
                .isNotNull();

        assertThat(restaurantCreated.getId()).isEqualTo(restaurant.getId());
        assertThat(restaurantCreated.getName()).isEqualTo(restaurant.getName());
        assertThat(restaurantCreated.getAddress()).isEqualTo(restaurant.getAddress());
        assertThat(restaurantCreated.getDescription()).isEqualTo(restaurant.getDescription());
        assertThat(restaurantCreated.getCuisine()).isEqualTo(restaurant.getCuisine());
        assertThat(restaurant.getId()).isNotNull();

        verify(restaurantRepository,times(1)).save(any(RestaurantModel.class));

    }

    private Restaurant createRestaurant(UUID id){
        List<OpeningHours>openingHours = new ArrayList<>();
        OpeningHours openingHours1 = new OpeningHours(DayOfWeek.MONDAY, LocalTime.now(),LocalTime.now());
        OpeningHours openingHours2 = new OpeningHours(DayOfWeek.FRIDAY, LocalTime.now(),LocalTime.now());
        OpeningHours openingHours3 = new OpeningHours(DayOfWeek.SATURDAY, LocalTime.now(),LocalTime.now());
        openingHours.add(openingHours2);
        openingHours.add(openingHours3);
        openingHours.add(openingHours1);
        List<Table> tables = new ArrayList<>();
        tables.add(createTable());
        var addres = new Address("street",
                123,
                "neighborhood",
                "city",
                "state",
                "country",
                "zipCode");

        return new Restaurant(id,
                "Restaurante",
                addres,
                "1187652435",
                "Restaurante italiano",
                Cuisine.ITALIAN,
                openingHours,tables);
    }

    private Table createTable(){

        return new Table(1,4, TableStatus.FREE);
    }
}