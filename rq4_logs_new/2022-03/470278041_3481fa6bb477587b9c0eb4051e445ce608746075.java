package by.TMS_Dudak.HomeTaskOOP.Task1;

import java.util.Random;

public class Phone {
    private int number;
    private String model;
    private int weight;

    public Phone() {
    }

    public Phone(int number, String model) {
        this.number = number;
        this.model = model;
    }

    Phone(int number, String model, int weight){
      this(number, model);
      this.weight=weight;
    }


    public void print(){
        System.out.println("Номер телефона: " + number + ", модель телефона: "
        + getModel() + ", вес телефона " + getWeight());
    }

    public  void receiveCall(){
        Random random = new Random();
        String randomName[] = new String[5];
        randomName[0] = "Maria";
        randomName[1] = "Victor";
        randomName[2] = "Artyom";
        randomName[3] = "Polina";
        randomName[4] = "Dmitriy";

        int i = random.nextInt(0,5);
        String callingName = randomName[i];

        System.out.println("Звонит " + callingName);

    }

    public  void receiveCall(int Number){
        Random random = new Random();
        String randomName[] = new String[5];
        randomName[0] = "Maria";
        randomName[1] = "Victor";
        randomName[2] = "Artyom";
        randomName[3] = "Polina";
        randomName[4] = "Dmitriy";

        int i = random.nextInt(0,5);
        String callingName = randomName[i];
        System.out.println("Звонит " + callingName + " по номеру " + Number);

    }


    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }
}