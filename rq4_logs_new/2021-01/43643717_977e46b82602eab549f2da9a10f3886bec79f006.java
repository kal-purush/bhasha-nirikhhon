package flowers;

import lombok.Builder;

@Builder
public class Bouquet {
    Flower[] flowers;
    String wrapping;
    String addons;
}