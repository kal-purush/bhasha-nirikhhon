package telegrambot.telegrambot.dataBase;

import javax.persistence.*;

@Entity
@Table(name = "users")
public class UserItem {

    private String firstName;
    private String lastName;
    @Id
    //@GeneratedValue(strategy = GenerationType.AUTO)
    private long chatId;

    public UserItem(String firstName, String lastName, long chatId) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.chatId = chatId;
    }

    public UserItem() {
    }


    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public long getChatId() {
        return chatId;
    }

    public void setChatId(int chatId) {
        this.chatId = chatId;
    }
}