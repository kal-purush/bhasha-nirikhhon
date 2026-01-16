package org.example.canon.entity;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Image {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String fileName;
    private String imageURL;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "post_id")
    private Post post;

    public Image(String fileName, String imageURL) {
        this.fileName = fileName;
        this.imageURL = imageURL;
    }

    public static Image of(Image image, Post post){
        return Image.builder()
                .fileName(image.getFileName())
                .imageURL(image.getImageURL())
                .post(post)
                .build();

    }
}