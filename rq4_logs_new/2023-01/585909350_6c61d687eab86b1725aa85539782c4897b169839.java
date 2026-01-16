package com.x29Naybla.Main;

public class Language {

    public static String day;
    public static String dead;
    public static String paused;

    public int currLang = 0;
    public int maxLang = 3;

    public static String[] options = {"0", "1", "2", "3"};

    public static boolean up;

    public void tick(){

        if(currLang == 0){
            //English selected
            day = "Day: ";
            dead = "You died";
            paused = "Paused";
            options[0] = "New world";
            options[1] = "Load world";
            options[2] = "Change language";
            options[3] = "Exit";

        }
        if(currLang == 1){
            //Portuguese selected
            day = "Dia: ";
            dead = "Você morreu";
            paused = "Pausado";
            options[0] = "Novo mundo";
            options[1] = "Carregar mundo";
            options[2] = "Mudar idioma";
            options[3] = "Sair";

        }
        if(currLang == 2){
            //French selected
            day = "Jour: ";
            dead = "Tu es mort";
            paused = "En pause";

            options[0] = "Nouveau monde";
            options[1] = "Charger monde";
            options[2] = "Changer la langue";
            options[3] = "Quitter";

        }
        if(currLang == 3){
            //German
            day = "Tag: ";
            dead = "Du bist gestorben";
            paused = "Pausiert";

            options[0] = "Neue Welt";
            options[1] = "Welt laden";
            options[2] = "Sprache ändern";
            options[3] = "Verlassen";

        }
    }

    public void changeLang(){
        if(up){
            currLang = currLang +1;
            System.out.println(currLang);
            up = false;
            if(currLang > maxLang){
                currLang = 0;
            }
        }
    }

}