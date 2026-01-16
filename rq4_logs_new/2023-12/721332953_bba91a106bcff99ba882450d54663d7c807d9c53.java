package model;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class ModelsTest {
    private static ClothingItem clothingItem;

    @BeforeAll
    static void setUp() {
        clothingItem = new ClothingItem(
                1L,
                "TestClothing",
                new Image(new File("testImage"), new byte[]{}),
                ClothingType.HEAD,
                10,
                Optional.of("Test Description")
        );
    }

    @Test
    void testIsAppropriateForTemperature() {
        assertTrue(clothingItem.isAppropriateForTemperature(15));
        assertFalse(clothingItem.isAppropriateForTemperature(5));
    }

    @Test
    void testEquals() {
        ClothingItem sameClothingItem = new ClothingItem(
                1L,
                "TestClothing",
                new Image(clothingItem.getImage().getImageFile(), new byte[]{}),
                ClothingType.HEAD,
                10,
                Optional.of("Test Description")
        );
        ClothingItem differentClothingItem = new ClothingItem(
                2L,
                "DifferentClothing",
                new Image(new File("testImage"), new byte[]{}),
                ClothingType.INNER_UPPER_BODY,
                20,
                Optional.of("Different Description")
        );

        assertEquals(clothingItem, sameClothingItem);
        assertNotEquals(clothingItem, differentClothingItem);
    }

    @Test
    void testSetDifferentFields() {
        ClothingItem clothingItem1 = new ClothingItem(
                2L,
                "DifferentClothing",
                new Image(new File("testImage"), new byte[]{}),
                ClothingType.INNER_UPPER_BODY,
                20,
                Optional.of("Different Description")
        );

        clothingItem1.setId(10L);
        clothingItem1.setClothingType(ClothingType.HEAD);
        clothingItem1.setDescription(Optional.of("Description"));
        clothingItem1.setImage(new Image(new File("testImage"), new byte[]{}));
        clothingItem1.setMinimumAppropriateTemperature(10);
        clothingItem1.setName("dasha");

//        clothingItem1.getImage().
    }

    @Test
    void testHashCode() {
        ClothingItem sameClothingItem = new ClothingItem(
                1L,
                "TestClothing",
                new Image(new File("testImage"), new byte[]{}),
                ClothingType.HEAD,
                10,
                Optional.of("Test Description")
        );
        assertEquals(clothingItem.hashCode(), sameClothingItem.hashCode());
    }

    @Test
    void testToString() {
        String expectedToString = "ClothingItem[id=1, name=TestClothing, image=Image{imageFile=testImage, imageData=[]}, " +
                "clothingType=HEAD, minimumAppropriateTemperature=10, " +
                "description=Optional[Test Description]]";

        assertEquals(expectedToString, clothingItem.toString());
    }

//    private static class MockImageIO {
//        static BufferedImage read(ByteArrayInputStream input) {
//            // Simulate reading an image
//            return new BufferedImage(200, 200, BufferedImage.TYPE_INT_RGB);
//        }
//
//        static boolean write(BufferedImage image, String formatName, ByteArrayOutputStream output) throws IOException {
//            // Simulate writing an image
//            return ImageIO.write(image, formatName, output);
//        }
//    }
//
//    @Test
//    void testGetScaledInstance() throws IOException {
//        // Create a test double for dependencies
//        File mockFile = new File("test.jpg");
//        byte[] originalImageData = new byte[]{/* original image data */};
//        Image originalImage = new Image(mockFile, originalImageData);
//
//        ByteArrayInputStream mockBis = new ByteArrayInputStream(originalImageData);
//        ByteArrayOutputStream mockBaos = new ByteArrayOutputStream();
//
//        // Replace the actual behavior with a test implementation
//        BufferedImage mockOriginalImage = MockImageIO.read(mockBis);
//        Graphics2D mockGraphics2D = mockOriginalImage.createGraphics();
//        // Create the Image instance
//        Image scaledImage = originalImage.getScaledInstance(100, 100, 1);
//
//        // Verify method calls (if needed)
//        // Assertions
//        assertEquals(originalImage.getImageFile(), scaledImage.getImageFile());
//        // Add more assertions based on your requirements
//    }
}