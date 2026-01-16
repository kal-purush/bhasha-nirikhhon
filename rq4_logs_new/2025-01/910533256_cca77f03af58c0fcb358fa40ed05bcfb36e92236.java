package tictactoe.domain.usecases;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javafx.scene.image.Image;


public class RandomAvatarUseCase {
    private final List<Image> avatarImages;
    private final Random random;

    public RandomAvatarUseCase() {
        this.avatarImages = loadAvatars();
        this.random = new Random();
    }
    
    private List<Image> loadAvatars() {
        List<Image> images = new ArrayList<>();
        for (int i = 1; i <= 15; i++) {
            try {
                String imagePath = String.format("/resources/images/avaters/img%d.jpg", i);
                images.add(new Image(getClass().getResource(imagePath).toExternalForm()));
            } catch (NullPointerException e) {
                System.out.println("Image not found: img" + i + ".jpg");
            }
        }
        return images;
    }
    
     public Image getRandomAvatar() {
        if (avatarImages.isEmpty()) {
            return null; 
        }
        return avatarImages.get(random.nextInt(avatarImages.size()));
    }
     
    
}