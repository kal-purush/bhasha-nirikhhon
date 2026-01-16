package jake.blog.inventoryApi.mappers;

import jake.blog.inventoryApi.model.db.PurchaseRecord;
import jake.blog.inventoryApi.model.dto.InboundPurchaseRecordDTO;
import jake.blog.inventoryApi.model.dto.OutboundPurchaseRecordDTO;
import jake.blog.inventoryApi.persist.StoreItemRepository;
import jake.blog.inventoryApi.testUtils.DefaultStoreValues;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

@Log4j2
class DtoMapperTest {

    private StoreItemRepository storeItemRepository;

    private DtoMapper objectUnderTest;

    @BeforeEach
    void setup() {
        storeItemRepository = Mockito.mock(StoreItemRepository.class);
        objectUnderTest = new DtoMapper(storeItemRepository);

        DefaultStoreValues.initialize(storeItemRepository);
    }

    @Test
    void toPurchaseRecordSuccess() {
        /**
         * Setup
         */
        final InboundPurchaseRecordDTO inboundPurchaseRecordDTO = DefaultStoreValues.defaultInboundCustomerPurchase;
        final PurchaseRecord expectedResult = DefaultStoreValues.defaultCustomerPurchaseRecord.setCreatedDate(null).setPurchaseID(null);

        /**
         * Exercise
         */
        final PurchaseRecord actualResult = objectUnderTest.toPurchaseRecord(inboundPurchaseRecordDTO);

        /**
         * Verify
         */
        Assertions.assertEquals(expectedResult, actualResult);
    }

    @Test
    void toPurchaseRecordFailStoreItemNotFound() {
        /**
         * Setup
         */
        // augment the default purchase with a rogue store item id with no matching item in the repository
        final long randomStoreItemID = 100;
        final Set<Long> purchasedItemIDs = DefaultStoreValues.defaultCustomerPurchaseItemIDs;
        purchasedItemIDs.add(randomStoreItemID);
        Mockito.when(storeItemRepository.findById(randomStoreItemID)).thenReturn(Optional.empty());
        final InboundPurchaseRecordDTO inboundPurchaseRecordDTO = new InboundPurchaseRecordDTO()
                .setCustomerID(DefaultStoreValues.defaultCustomerID1)
                .setPurchasedItems(purchasedItemIDs);

        /**
         * Exercise
         */
        try {
            final PurchaseRecord actualResult = objectUnderTest.toPurchaseRecord(inboundPurchaseRecordDTO);
        } catch(final NoSuchElementException e) {
            /**
             * Verify
             */
            log.error("Exception: ", e);
            Assertions.assertTrue(true, "Exception occurred as expected when item not found in store repository");
            return;
        }
        /**
         * Verify
         */
        Assertions.assertTrue(false, "Exception did not occur as expected when item not found in store repository");
    }

    @Test
    void toOutboundPurchaseRecordDtoSuccess() {
        /**
         * Setup
         */
        final PurchaseRecord purchaseRecord = DefaultStoreValues.defaultCustomerPurchaseRecord;
        final Float totalCost = 55.55f;

        /**
         * Exercise
         */
        final OutboundPurchaseRecordDTO actualResult = objectUnderTest.toOutboundPurchaseRecordDTO(purchaseRecord, totalCost);

        /**
         * Verify
         */
    }

}