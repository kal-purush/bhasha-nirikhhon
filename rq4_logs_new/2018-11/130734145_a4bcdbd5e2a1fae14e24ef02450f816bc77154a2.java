package com.application.restaurantbooking;

import com.application.restaurantbooking.controllers.ReservationController;
import com.application.restaurantbooking.controllers.RestaurantController;
import com.application.restaurantbooking.controllers.UrlRequests;
import com.application.restaurantbooking.persistence.builder.*;
import com.application.restaurantbooking.persistence.model.*;
import com.application.restaurantbooking.persistence.service.RestaurantService;
import com.application.restaurantbooking.persistence.service.UserServiceManager;
import com.application.restaurantbooking.utils.RestaurantSearcher;
import com.application.restaurantbooking.utils.TableSearcher;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.util.*;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class RestControllersTest {

    private MockMvc mockMvc;

    @InjectMocks
    private RestaurantController restaurantController;

    @InjectMocks
    private ReservationController reservationController;

    @Mock
    private UserServiceManager userServiceManager;

    @Mock
    private RestaurantSearcher restaurantSearcher;

    @Mock
    private TableSearcher tableSearcher;

    @Mock
    private RestaurantService restaurantService;

    private Restaurant restaurant;

    private Reservation reservation = new Reservation();

    public RestControllersTest() {
        MockitoAnnotations.initMocks(this);
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
        Map<DayOfWeek, OpenHours> openHours = new EnumMap<>(DayOfWeek.class);
        try {
            for (DayOfWeek dayOfWeek : DayOfWeek.values()) {
                OpenHours day = new OpenHoursBuilder()
                        .openHour(sdf.parse("12:00"))
                        .closeHour(sdf.parse("22:00"))
                        .isClose(false)
                        .build();
                day.setId(1L);
                openHours.put(dayOfWeek, day);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Restorer restorer = new RestorerBuilder().username("test").password("test").build();
        Client client = new ClientBuilder().username("client").password("client").build();
        restaurant = new RestaurantBuilder()
                .name("name")
                .city("city")
                .phoneNumber("123")
                .street("street")
                .streetNumber("1")
                .price(Price.HIGH)
                .tags(Sets.newHashSet(Tag.SEAFOOD))
                .website("web")
                .openHours(openHours)
                .restorer(restorer)
                .localization(1.0, 2.0)
                .build();
        restorer.setRestaurant(restaurant);
        restaurant.setId(1L);
        RestaurantTable table = new RestaurantTableBuilder().restaurant(restaurant).maxPlaces(10).build();
        table.setId(1L);
        try {
            reservation = new ReservationBuilder().client(client).comment("comment").cancelled(false)
                    .reservedPlaces(5).reservationLength(60).restaurantTable(table).reservationDate("2018-06-15_17:00")
                    .build();
         } catch (ParseException e) {
            e.printStackTrace();
        }
        client.setReservations(Sets.newHashSet(reservation));
        Mockito.when(userServiceManager.getRestorerByJwt(any())).thenReturn(restaurant.getRestorer());
        Mockito.when(userServiceManager.getClientByJwt(any())).thenReturn(client);
        Mockito.when(restaurantSearcher.getRestaurantsByQuery(any(), any())).thenReturn(Collections.singletonList(restaurant));
        Mockito.when(tableSearcher.getNearHoursReservation(any(), any())).thenReturn(Arrays.asList("12:00", "12:30", "13:00", "13:30", "14:00"));
        Mockito.when(restaurantService.getById(any())).thenReturn(restaurant);
    }

    @Before
    public void init(){
        mockMvc = MockMvcBuilders
                .standaloneSetup(restaurantController, reservationController)
                .build();
    }

    @Test
    public void getTagsTest() throws Exception {
        List<Tag> tags = Arrays.asList(Tag.values());
        mockMvc.perform(get(UrlRequests.GET_TAGS)
                .contentType(APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$", hasSize(Tag.values().length)))
                .andExpect(jsonPath("$[0]").value(tags.get(0).toString()))
                .andExpect(jsonPath("$[1]").value(tags.get(1).toString()))
                .andExpect(jsonPath("$[2]").value(tags.get(2).toString()));
    }

    @Test
    public void restaurantDTOTest() throws Exception {
        mockMvc.perform(get(UrlRequests.GET_RESTAURANT_BY_RESTORER)
                .contentType(APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(restaurant.getId()))
                .andExpect(jsonPath("$.name").value(restaurant.getName()))
                .andExpect(jsonPath("$.city").value(restaurant.getCity()))
                .andExpect(jsonPath("$.latitude").value(restaurant.getLatitude()))
                .andExpect(jsonPath("$.longitude").value(restaurant.getLongitude()))
                .andExpect(jsonPath("$.street").value(restaurant.getStreet()))
                .andExpect(jsonPath("$.streetNumber").value(restaurant.getStreetNumber()))
                .andExpect(jsonPath("$.website").value(restaurant.getWebsite()))
                .andExpect(jsonPath("$.priority").value(restaurant.getPriority()))
                .andExpect(jsonPath("$.phoneNumber").value(restaurant.getPhoneNumber()))
                .andExpect(jsonPath("$.restaurantPrice").isArray())
                .andExpect(jsonPath("$.restaurantPrice[0]").value(restaurant.getPrice().toString()))
                .andExpect(jsonPath("$.tags").isArray())
                .andExpect(jsonPath("$.tags[0]").value(restaurant.getTags().toArray()[0].toString()));
    }

    @Test
    public void openHoursDTOTest() throws Exception {
        mockMvc.perform(get(UrlRequests.GET_OPEN_HOURS_ALL)
                .contentType(APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.MONDAY.isClose").value(restaurant.getOpenHoursMap()
                        .get(DayOfWeek.MONDAY).getIsClose()))
                .andExpect(jsonPath("$.SATURDAY.id").value(restaurant.getOpenHoursMap()
                        .get(DayOfWeek.SATURDAY).getId()))
                .andExpect(jsonPath("$.SUNDAY.openHour").exists())
                .andExpect(jsonPath("$.WEDNESDAY.closeHour").exists());
    }

    @Test
    public void getPricesTest() throws Exception {
        List<Price> prices = Arrays.asList(Price.values());
        mockMvc.perform(get(UrlRequests.GET_PRICES)
                .contentType(APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$", hasSize(Price.values().length)))
                .andExpect(jsonPath("$[0]").value(prices.get(0).toString()))
                .andExpect(jsonPath("$[1]").value(prices.get(1).toString()))
                .andExpect(jsonPath("$[2]").value(prices.get(2).toString()));
    }

    @Test
    public void restaurantSearchQueryResultDTOTest() throws Exception {
        mockMvc.perform(get(UrlRequests.GET_SEARCH_QUERY_RESTAURANTS)
                .contentType(APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.restaurants").isArray())
                .andExpect(jsonPath("$.restaurants[0].id").value(restaurant.getId()))
                .andExpect(jsonPath("$.restaurants[0].name").value(restaurant.getName()))
                .andExpect(jsonPath("$.restaurants[0].city").value(restaurant.getCity()))
                .andExpect(jsonPath("$.restaurants[0].latitude").value(restaurant.getLatitude()))
                .andExpect(jsonPath("$.restaurants[0].longitude").value(restaurant.getLongitude()))
                .andExpect(jsonPath("$.restaurants[0].street").value(restaurant.getStreet()))
                .andExpect(jsonPath("$.restaurants[0].streetNumber").value(restaurant.getStreetNumber()))
                .andExpect(jsonPath("$.restaurants[0].website").value(restaurant.getWebsite()))
                .andExpect(jsonPath("$.restaurants[0].priority").value(restaurant.getPriority()))
                .andExpect(jsonPath("$.restaurants[0].phoneNumber").value(restaurant.getPhoneNumber()))
                .andExpect(jsonPath("$.restaurants[0].restaurantPrice").isArray())
                .andExpect(jsonPath("$.restaurants[0].restaurantPrice[0]").value(restaurant.getPrice().toString()))
                .andExpect(jsonPath("$.restaurants[0].tags").isArray())
                .andExpect(jsonPath("$.restaurants[0].tags[0]").value(restaurant.getTags().toArray()[0].toString()));
    }

    @Test
    public void reservationsDTOTest() throws Exception {
        mockMvc.perform(get(UrlRequests.GET_RESERVATIONS_LIST)
                .contentType(APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.reservations").isArray())
                .andExpect(jsonPath("$.reservations[0].id").value(reservation.getId()))
                .andExpect(jsonPath("$.reservations[0].cancelled").value(reservation.getIsCancelled()))
                .andExpect(jsonPath("$.reservations[0].comment").value(reservation.getComment()))
                .andExpect(jsonPath("$.reservations[0].dateReservation").value(reservation.getDateReservation()))
                .andExpect(jsonPath("$.reservations[0].reservationLength").value(reservation.getReservationLength()))
                .andExpect(jsonPath("$.reservations[0].restaurantId").value(reservation.getRestaurantId()))
                .andExpect(jsonPath("$.reservations[0].restaurantTableId").value(reservation.getRestaurantTable().getId()))
                .andExpect(jsonPath("$.reservations[0].reservedPlaces").value(reservation.getReservedPlaces()));
    }

}