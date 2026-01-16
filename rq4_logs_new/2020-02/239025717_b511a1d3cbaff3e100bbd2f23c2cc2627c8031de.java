package CoffeeShopSimulator;

import CoffeeShopSimulator.Utilities.ILogger;
import CoffeeShopSimulator.Utilities.Logger;
import com.google.common.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoffeeShopTest {
    private ILogger logger;
    private EventBus eventBus;
    private CoffeeShop coffeeShop;

    @BeforeEach
    void setUp() {
        logger = new Logger();
        eventBus = new EventBus("TestingEventBus");
        coffeeShop = new CoffeeShop(eventBus, logger);
    }

    @Test
    void shouldBuildTheEntireAppProperly() {
        coffeeShop.start();
    }
}