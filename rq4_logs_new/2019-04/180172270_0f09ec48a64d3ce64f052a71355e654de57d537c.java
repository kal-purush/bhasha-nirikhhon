package HW12;


import java.util.ArrayList;
import java.util.List;

public class Application {
    public static void main(String[] args) {
        UtubeParser parser = new UtubeParser();
        parser.init();
        List<String> listOfVideoId = new ArrayList<>();
        listOfVideoId.add("YTOZBSOMNsk");
        parser.getCommentsFromVideos(listOfVideoId);

        for (UtubeVideo video : parser.getListOfVideos()) {
            for (Comment comment : video.getComments()) {
                System.out.println("Author: " + comment.getNameAuthor());
                System.out.println("Comment: " + comment.getTextComment());
                System.out.println("Date of last modification: " + comment.getDateOfLastModification());
                System.out.println("Likes: " + comment.getLikes());
                System.out.println("");
            }
        }
    }

}