package inheritance;

public class MovieReview extends Review {
    String movie;
    public MovieReview(String author, int numberOfStars, String body, String movie) {
        super(author, numberOfStars, body);
        this.movie=movie;
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    @Override
    public String toString() {
        return "MovieReview{" +
                "author='" + getAuthor() + '\'' +
                ", numberOfStars=" + getNumberOfStars() + '\'' +
                ", body='" + getAuthor() + '\'' +
                ", movie='" + movie + '\'' +
                '}';
    }
}