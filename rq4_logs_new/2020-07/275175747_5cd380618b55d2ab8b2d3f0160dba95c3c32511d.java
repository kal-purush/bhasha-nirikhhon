package jm.config.initializer;

import jm.Item;
import jm.User;
import jm.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;

public class EntityDataInitializer {
    private static final Logger logger = LoggerFactory.getLogger(EntityDataInitializer.class);

    @Autowired
    private UserService userService;

    //TODO: временные заглушки
//    @Autowired
//    private CurrencyDao currencyDao;

//    @Autowired
//    private ItemDao itemDao;

//    @Autowired
//    private PurchaseInfoDao purchaseInfoDao;

//    @Autowired
//    private ShoeSizeDao shoeSizeDao;

    private void init() {
        logger.info("Data init has been started!!!");
        dataInit();
        logger.info("Data init has been done!!!");
    }

    private void dataInit() {
        createUsers();
    }

    private void createUsers() {

        User userVasya = new User();

        userVasya.setEmail("vasya.pupkin@email.com");
        userVasya.setUsername("vasya.pupkin");
        userVasya.setPassword("1");
        userVasya.setFirstName("Vasya");
        userVasya.setLastName("Pupkin");
        userVasya.setSellerLevel((byte) 10);
        userVasya.setVacationMode(true);

        userService.createUser(userVasya);


        User userPetya = new User();

        userPetya.setEmail("petya.vakulov@email.com");
        userPetya.setUsername("petya.vakulov");
        userPetya.setPassword("1");
        userPetya.setFirstName("Petya");
        userPetya.setLastName("Vakulov");
        userPetya.setSellerLevel((byte) 12);
        userPetya.setVacationMode(true);

        userService.createUser(userPetya);


        User userSemen = new User();

        userSemen.setEmail("semen.semenych@email.com");
        userSemen.setUsername("semen.semenych");
        userSemen.setPassword("1");
        userSemen.setFirstName("Semen");
        userSemen.setLastName("Semenych");
        userSemen.setSellerLevel((byte) 22);
        userSemen.setVacationMode(true);

        userService.createUser(userSemen);
    }

    private void createItems() {
        Item item1 = new Item();

        item1.setName("Jordan 14 Retro Gym Red Toro");
        item1.setPrice(190.0);
        item1.setCondition("New");
        item1.setDateRelease(LocalDate.of(2020, 7,2));
        item1.setHighestBid(316.0);
        item1.setLowestAsk(254.0);

        // size 12
//        itemDao.create(item1);

        Item item2 = new Item();

        item2.setName("adidas Yeezy Boost 380 Mist");
        item2.setPrice(230.0);
        item2.setCondition("New");
        item2.setDateRelease(LocalDate.of(2020,3,25));
        item2.setHighestBid(230.0);
        item2.setLowestAsk(195.0);

        //size 8.5
//        itemDao.create(item2);

        Item item3 = new Item();

        item3.setName("Nike React Element 87 Anthracite Black");
        item3.setPrice(160.0);
        item3.setCondition("New");
        item3.setDateRelease(LocalDate.of(2018,6,14));
        item3.setHighestBid(101.0);
        item3.setLowestAsk(77.0);

        //size 9
//        itemDao.create(item3);

        Item item4 = new Item();

        item4.setName("Jordan 4 Retro Winterized Loyal Blue");
        item4.setPrice(200.0);
        item4.setCondition("New");
        item4.setDateRelease(LocalDate.of(2019,12,21));
        item4.setHighestBid(212.0);
        item4.setLowestAsk(155.0);

        //size 7.5
//        itemDao.create(item4);

        Item item5 = new Item();

        item5.setName("Jordan 1 Retro High Satin Black Toe (W)");
        item5.setPrice(160.0);
        item5.setCondition("New");
        item5.setDateRelease(LocalDate.of(2019,8,17));
        item5.setHighestBid(442.0);
        item5.setLowestAsk(342.0);

        //size 11W
//        itemDao.create(item5);

    }

}