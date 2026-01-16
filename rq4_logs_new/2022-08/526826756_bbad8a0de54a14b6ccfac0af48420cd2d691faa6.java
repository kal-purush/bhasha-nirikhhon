package sky.pro.telegrambotforpets.model;

import javax.persistence.*;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Сущность <b>DogHandler</b> содержит информация о кинологе. <br>
 * Отображается на таблицу <b>doghandler</b>
 */
@Entity
@Table(name = "doghandler")
public class DogHandler {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "doghandler_chatid")
    private Long idChat;
    // doghandler_chatid BIGINT;

    @Column(name = "doghandler_name")
    private String name;
    //doghandler_name VARCHAR (20),

    @Column(name = "doghandler_middlename")
    private String middleName;
//    doghandler_middlename VARCHAR (20),

    @Column(name = "doghandler_lastname")
    private String lastName;
//    doghandler_lastname VARCHAR (20),

    @Column(name = "doghandler_gender")
    private Character gender;
//    doghandler_gender VARCHAR (1),

    @Column(name = "doghandler_datebirth")
    private LocalDate birthDate;
//    doghandler_datebirth DATE,

    @Column(name = "doghandler_telephone")
    private String phoneNumber;
//    doghandler_telephone VARCHAR (20),

    @Column(name = "doghandler_adress")
    private String adress;
//    doghandler_adress VARCHAR (255),

    @Column(name = "doghandler_description")
    private String description;
//    doghandler_description VARCHAR (255)

    public DogHandler() {
    }

    public Long getId() {
        return id;
    }

    public Long getIdChat() {
        return idChat;
    }

    public void setIdChat(Long idChat) {
        this.idChat = idChat;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMiddleName() {
        return middleName;
    }

    public void setMiddleName(String middleName) {
        this.middleName = middleName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Character getGender() {
        return gender;
    }

    public void setGender(Character gender) {
        this.gender = gender;
    }

    public LocalDate getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(LocalDate birthDate) {
        this.birthDate = birthDate;
    }

    public String getPhone() {
        return phoneNumber;
    }

    public void setPhone(String phone) {
        this.phoneNumber = phone;
    }

    public String getAdress() {
        return adress;
    }

    public void setAdress(String adress) {
        this.adress = adress;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DogHandler that = (DogHandler) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "DogHandler{" +
                "id=" + id +
                ", idChat=" + idChat +
                ", name='" + name + '\'' +
                ", middleName='" + middleName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", gender=" + gender +
                ", birthDate=" + birthDate +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", adress='" + adress + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}