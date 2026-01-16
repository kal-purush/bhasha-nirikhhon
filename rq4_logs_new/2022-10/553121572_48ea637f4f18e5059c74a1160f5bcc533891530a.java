package transport;

import java.sql.Struct;
import java.time.LocalDate;

public class Car {
   private final String brand;
    private final String model;
    private double engineVolume;
    private String color;
    private final String productionCountry;
    private final int productionYear;
    private String transmission;
    private final String bodyType;
    private  String registrationNumber;
    private final int numberOfSeats;
    private boolean winterTires;
    private final Key key;
    private final Insurence insurence;





    public Car(String brand, String model, double engineVolume,
               String color, int productionYear, String productionCountry,
               String transmission,int numberOfSeats,String registrationNumber,
               String bodyType, boolean winterTires,Key key,Insurence insurence) {
        this.brand = (brand == null||brand.isEmpty()||brand.isBlank()) ? "--default--" : brand;
        this.model = (model == null||model.isEmpty()||model.isBlank()) ? "--default--" : model;
        setEngineVolume(engineVolume);
        setColor(color);
        this.productionYear = (productionYear <= 1999) ? 2000 : productionYear;
        this.productionCountry = (productionCountry == null||productionCountry.isEmpty()||productionCountry.isBlank()) ? "--default--" : productionCountry;
        setTransmission(transmission);
        this.numberOfSeats = (numberOfSeats <= 0 ) ? 4 : numberOfSeats;
        this.registrationNumber = registrationNumber;
        this.bodyType = (bodyType == null||bodyType.isEmpty()||bodyType.isBlank()) ? "--default--" : bodyType;;
        setWinterTires(winterTires);
        this.key = (key == null) ? new Key(true,true) : key;
        this.insurence = (insurence == null) ? new Insurence(null, 10030,"234556967") : insurence;


    }

    public boolean isRegistrationNumber() {
        if (this.registrationNumber.length() != 9) {
            return false;
        }
        char[] regNumberChars = this.registrationNumber.toCharArray();
        return isNumberLetter(regNumberChars[0])
                && isNumber(regNumberChars[1])
                && isNumber(regNumberChars[2])
                && isNumber(regNumberChars[3])
                && isNumber(regNumberChars[4])
                && isNumber(regNumberChars[5])
                && isNumber(regNumberChars[6])
                && isNumber(regNumberChars[7])
                && isNumber(regNumberChars[8]);
    }

    private boolean isNumber(char symbol) {
        return Character.isDigit(symbol);
    }

    private boolean isNumberLetter(char symbol) {
        String allowedSymbols = "АВЕКМНОРСТУХ";
        return allowedSymbols.contains("" + symbol);
    }
    public void setSeasonTires() {
        int currentMonth = LocalDate.now().getMonthValue();

        this.winterTires = currentMonth <= 4 || currentMonth >= 11;
    }

    public boolean isWinterTires() {
        return winterTires;
    }

    public void setWinterTires(boolean winterTires) {
        this.winterTires = winterTires;
    }

    public String getRegistrationNumber() {
        return registrationNumber;
    }

    public void setRegistrationNumber(String registrationNumber) {
        this.registrationNumber = registrationNumber;
    }

    public String getTransmission() {
        return transmission;
    }

    public void setTransmission(String transmission) {
        this.transmission = (transmission == null||transmission.isEmpty()||transmission.isBlank()) ? "--default--" : transmission;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = (color == null||color.isEmpty()||color.isBlank()) ? "белый" : color;
    }

    public double getEngineVolume() {
        return engineVolume;
    }

    public void setEngineVolume(double engineVolume) {
        this.engineVolume = (engineVolume <= 0 ) ? 1.5 : engineVolume;
    }

    public String getBrand() {
        return brand;
    }

    public String getModel() {
        return model;
    }

    public int getProductionYear() {
        return productionYear;
    }

    public String getProductionCountry() {
        return productionCountry;
    }

    public String getBodyType() {
        return bodyType;
    }

    public int getNumberOfSeats() {
        return numberOfSeats;
    }

    public void characteristics() {
        System.out.println(getBrand() + " " + getModel() + ". Объём двигателя - " + getEngineVolume() +
                ". " + "Цвет кузова - " + getColor() + ". Год выпуска - "
                + getProductionYear() + ". Сборка - " +  getProductionCountry() + ". " + getTransmission() + " - коробка передач, " +
                "тип кузова - " + getBodyType() +
                ", регистрационный номер - " + getRegistrationNumber() +
                ", количество мест - " + getNumberOfSeats() +
                ", тип шин - " + (winterTires ? "летняя, " : "зимняя, ") +
                (getKey().isRemoteEngineStart() ? "удаленный запуск двигателя, " : "бесключевой доступ") +
                ", " + (getKey().isKeylessAccess() ? "бесключевой доступ, " : " удаленный запуск двигателя") +
                ", номер страховки - " + getInsurence().getNumber() +
                ", стоимость страховки - " + getInsurence().getCost() +
                " руб., срок действия - " + getInsurence().getValidUntil());
    }




    public  static  class  Key {
        private final boolean remoteEngineStart;
        private final boolean keylessAccess;

    public Key(boolean remoteEngineStart, boolean keylessAccess) {
        this.remoteEngineStart = remoteEngineStart;
        this.keylessAccess = keylessAccess;
    }

    public boolean isRemoteEngineStart() {
        return remoteEngineStart;
    }

        public boolean isKeylessAccess() {
        return  keylessAccess;
        }
    }

    public Key getKey() {
        return key;
    }

    public Insurence getInsurence() {
        return insurence;
    }

    public static class  Insurence{
        private final LocalDate validUntil;
        private final double cost;
        private final String number;

        public Insurence(LocalDate validUntil, double cost, String number) {
            this.validUntil = validUntil != null ? validUntil : LocalDate.now().plusDays(10);
            this.cost = Math.max(cost,1);
            this.number = (number == null||number.isEmpty()||number.isBlank()) ? "--default--" : number;;
        }


        public double getCost() {
            return cost;
        }

        public LocalDate getValidUntil() {
            return validUntil;
        }

        public String getNumber() {
            return number;
        }

        boolean isNumberValid() {
            return number.length() == 9;
        }

        public boolean isInsuranceValid() {
            return LocalDate.now().isBefore(this.validUntil);
        }

    }

}