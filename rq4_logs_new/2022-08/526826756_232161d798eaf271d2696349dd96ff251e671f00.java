package sky.pro.telegrambotforpets.model;

import java.util.Date;
import java.util.Objects;

/**
 * вводим абстаркатный класс, чтобы впоследствии от него можно было наследовать Усыновителя,
 * Кинолога и, возможно, Волонтера
 */
public abstract class Person {
    private String name;
    private String middleName;
    private String lastName;
    private String gender;
    private Date birthday;
    private String phoneNumber;
    private String address;


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

    public String getGender() {
        return gender;
    }

    /**
     * здесь ввел Enum, т.к. думаю возможны и другие варианты обозначения пола: "W", "Ж", "М".
     * если вводить их, то Enum будет удобен
     */
    private enum Gender {
        M, F
    }

    public void setGender(String gender) {
        if (gender.length() == 1 && (
                gender.toUpperCase().equals(Gender.M.name()) || gender.toUpperCase().equals(Gender.F.name())
        )) {
            this.gender = gender;
        } else {
            throw new IllegalArgumentException("введены недопустимые данные");
        }
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return name.equals(person.name) && middleName.equals(person.middleName) && lastName.equals(person.lastName) && gender.equals(person.gender) && birthday.equals(person.birthday) && phoneNumber.equals(person.phoneNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, middleName, lastName, gender, birthday, phoneNumber);
    }

}