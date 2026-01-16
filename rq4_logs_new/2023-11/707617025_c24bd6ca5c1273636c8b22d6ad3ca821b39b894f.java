package csvmetricprocessor;

public class EpisoDialog {
    private int number;
    private int time_stamp;
    private String character;
    private String location;

    private String text;

    EpisoDialog(String[] data){
        number = Integer.parseInt(data[2]);
        time_stamp = Integer.parseInt(data[3]);
        character = data[4];
        location = data[5];
        text = data[6];

    }

    public int getNumber() {
        return number;
    }

    public int getTime_stamp() {
        return time_stamp;
    }

    public String getCharacter() {
        return character;
    }

    public String getLocation() {
        return location;
    }

    public String getText() {
        return text;
    }
}