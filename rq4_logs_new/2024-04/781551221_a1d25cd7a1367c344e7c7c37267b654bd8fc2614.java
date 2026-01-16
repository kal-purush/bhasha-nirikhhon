import java.util.*;
class Movie {
    private String title;
    private String rating;
    private double ratingValue;
    public Movie(String title, String rating, double ratingValue) {
        this.title = title;
        this.rating = rating;
        this.ratingValue = ratingValue;
    }
    public String getTitle() {
        return title;
    }
    public String getRating() {
        return rating;
    }
    public double getRatingValue() {
        return ratingValue;
    }
}
public class MovieRatingAnalyzer {
    public static void main(String[] args) {
        List<Movie> movies = new ArrayList<>();
        movies.add(new Movie("Movie 1", "PG", 4.5));
        movies.add(new Movie("Movie 2", "PG-13", 3.8));
        movies.add(new Movie("Movie 3", "R", 4.2));
        movies.add(new Movie("Movie 4", "PG", 3.9));
        movies.add(new Movie("Movie 5", "PG-13", 4.1));
        movies.add(new Movie("Movie 6", "R", 3.7));
        Map<String, Integer> categoryCounts = new HashMap<>();
        Map<String, Double> categoryTotalRatings = new HashMap<>();
        for (Movie movie : movies) {
            String ratingCategory = movie.getRating();
            double ratingValue = movie.getRatingValue();
            categoryCounts.put(ratingCategory, categoryCounts.getOrDefault(ratingCategory, 0) + 1);
            categoryTotalRatings.put(ratingCategory, categoryTotalRatings.getOrDefault(ratingCategory, 0.0) + ratingValue);
        }
        Map<String, Double> categoryAverageRatings = new HashMap<>();
        for (String category : categoryCounts.keySet()) {
            int count = categoryCounts.get(category);
            double totalRating = categoryTotalRatings.get(category);
            double avgRating = totalRating / count;
            categoryAverageRatings.put(category, avgRating);
        }
        System.out.println("Movie Ratings Statistics:");
        for (String category : categoryCounts.keySet()) {
            int count = categoryCounts.get(category);
            double avgRating = categoryAverageRatings.get(category);
            System.out.println("Category: " + category);
            System.out.println("Number of Movies: " + count);
            System.out.println("Average Rating: " + avgRating);
            System.out.println();
        }
    }
}