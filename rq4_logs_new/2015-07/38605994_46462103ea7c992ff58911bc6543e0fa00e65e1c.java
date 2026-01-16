package org.example;

public interface IFearable {

    public String characterName();
    public Character lordAbbreviation();
    public Home showYourNation();


    /*Pamietaj, że w interfacach pola oraz jakiekolwiek klasy sa domyslnie 'public' i 'static'
    a dodatkowo te pola czy klasy zostana odziecziczone przez implementacje interfacu*/

    public static enum Home {
        Isengard("Saruman"), Mordor("Nazgul"), Gondor("Faramir"), Rohan("Eomer"), Shire("Frodo");

        String name;

        private Home(String masterName) {
            this.name = masterName;
        }

        public String whatsYourMaster() {
            return this.name;
        }

    }

}