package Models;

import java.util.ArrayList;

/**
 * Created by debrian on 1/21/17.
 */

public class Character {

        //Scanner input = new Scanner(System.in);
        private int str;
        private int intelli;
        private int dex;
        private int consti;
        private int wis;
        private int charisma;

        private String name;
        private String race;
        private String profession;

        private int wealth;
        private int hitPoints;
        private int level;
        private int hitDie;
        private int exp;
        //ArrayList for Languages and profencies
        ArrayList<String> proficiencies = new ArrayList<String>();
        //Create ArrayList for equipment and show equipment
        ArrayList<String> equipment = new ArrayList<String>();

        //Create ArrayList for spells, Cantrips, level 1 and level 2..etc.
        ArrayList<String> spells = new ArrayList<String>();
        //A Long_Rest method that resets spell count
        //Counter to count number of times spell has been casted...Cast spell method
        //Constructor for the character
        Character(String name, String race){
            str = 0;
            intelli = 0;
            dex = 0;
            consti= 0;
            wis = 0;
            charisma = 0;
            wealth = 0;
            hitPoints = 0;
            exp = 0;
            this.name = name;
            this.race = race;
            profession = "";
        }
        //Getters and Setters
        public int getStr(){
            return str;
        }
        public int getIntelli(){
            return intelli;
        }
        public int getDex(){
            return dex;
        }
        public int getConsti(){
            return consti;
        }
        public int getWis(){
            return wis;
        }
        public int getCharisma(){
            return charisma;
        }
        public String getName(){
            return name;
        }
        public void setProfession(String profession){
            this.profession = profession;
        }
        public String getProfession() {
            return profession;
        }
        public int getHP() {
            return hitPoints;
        }
        public int getWealth(){
            return wealth;
        }
        public int getExp(){
            return exp;
        }
//        public ArrayList<String> addSpells(ArrayList<String> spells, Scanner input){
//            System.out.println("How many spells would you like to add?");
//            int count = input.nextInt();
//            for(int i = 0; i <= count; i++){
//                System.out.println("Enter a spell to add");
//                spells.add(input.next());
//            }
//            return spells;
//        }
//        public ArrayList<String> addEquipment(ArrayList<String> equipment, Scanner input){
//            System.out.println("How many items would you like to add?");
//            int count = input.nextInt();
//            for(int i = 0; i <= count; i++){
//                System.out.println("Enter an item to add to your inventory");
//                equipment.add(input.next());
//            }
//            return equipment;
//        }
//        public ArrayList<String> addProficiences(ArrayList<String> spells, Scanner input){
//            System.out.println("How many spells would you like to add?");
//            int count = input.nextInt();
//            for(int i = 0; i <= count; i++){
//                System.out.println("Enter a spell to add");
//                proficiencies.add(input.next());
//            }
//            return spells;
//        }
//        public ArrayList<String> removeSpells(ArrayList<String> spells, Scanner input){
//            System.out.println("How many spells would you like to add?");
//            int count = input.nextInt();
//            for(int i = 0; i <= count; i++){
//                System.out.println("Enter a spell to remove");
//                spells.remove(input.next());
//            }
//            return spells;
//        }
//        public ArrayList<String> removeEquipment(ArrayList<String> equipment, Scanner input){
//            System.out.println("How many items would you like to add?");
//            int count = input.nextInt();
//            for(int i = 0; i <= count; i++){
//                System.out.println("Enter an item to add to your inventory");
//                equipment.remove(input.next());
//            }
//            return equipment;
//        }
//        public boolean short_Rest(){
//            //Able to spend hit die up to char level
//        }
//        public boolean long_Rest(){
//            //Regains lost hit points and gets hit dice equal to half their level
//            //Spells can be chosen again
//            //addSpells(spells, input);
//
//        }
//        //Add Experience Points
//        public int addXP(Scanner input){
//            System.out.println("Please enter the amount of experience you will add");
//            int newXP = input.nextInt();
//            exp =+ newXP;
//            System.out.println("You're new experience is: ");
//            return exp;
//        }
        //Calculate level based on experience points
        //
        public boolean addRaceStats() {
            //Adds Ability Score modifiers to character given their race
            //Series of If/Else Statements
            if(race.equals("Human")){
                str =+ 1;
                intelli =+ 1;
                dex =+ 1;
                consti =+ 1;
                wis =+ 1;
                charisma =+ 1;
                return true;
            }
            else if(race.equals("Halfling")){
                dex =+ 2;
                return true;
            }
            else if(race.equals("Elf")){
                dex =+ 2;
                return true;
            }
            else if(race.equals("Dwarf")){
                consti =+ 2;
                return true;
            }
            else if(race.equals("Tiefling")){
                intelli =+ 1;
                charisma =+ 2;
                return true;
            }
            else
                return false;
        }
        //Adds HP and Wealth for Charcacters given their class
        public void addHPAndWealth() {
            if(profession.equals("Fighter")){
                for(int i = 0; i <= 5; i++){
                    wealth =+ (int) Math.random() * 4 + 1;
                }
                wealth = wealth*10;
                hitPoints = 10 + checkModifier(consti);
            }
            else if(profession.equalsIgnoreCase("Wizard")){
                for(int i = 0; i <= 4; i++){
                    wealth =+ (int) Math.random() * 4 + 1;
                }
                wealth = wealth*10;
                hitPoints = 6 + checkModifier(consti);
            }
            else if(profession.equalsIgnoreCase("Rogue")){
                for(int i = 0; i <= 4; i++){
                    wealth =+ (int) Math.random() * 4 + 1;
                }
                wealth = wealth*10;
                hitPoints = 8+ checkModifier(consti);
            }
            else if(profession.equalsIgnoreCase("Cleric")){
                for(int i = 0; i <= 5; i++){
                    wealth =+ (int) Math.random() * 4;
                }
                wealth = wealth*10;
                hitPoints = 8 + checkModifier(consti);
            }
            else{
                System.out.println("No information for that class.");
            }
        }
//        public void createStats(Scanner input){
//            System.out.println("How would you like to create ability Scores?");
//            System.out.print("(Random(1), stanard set(2), Point Buy(3))");
//            int createMethod = input.nextInt();
//            if(createMethod == 1){
//
//            }
//            System.out.println("Now, rolling dice, will choose"
//                    + " highest of 4 rolls");
//
//
//        }
        public void randomStats() {
            int[] points = new int[4];
            int sum;
            for(int i = 0; i <= 4; i++) {
                points[i] = (int) (Math.random() * 6) + 1;
                if(points[i] > points[i-1]){
                    sum =+ points[i];
                }
            }
//		System.out.println(sum);
        }
        //Uses user input to apply stats to various Ability Scores
//        public void standardSet(Scanner input){
//            while(true) {
//                System.out.println("Strength: 0");
//                System.out.println("Intelligence: 0");
//                System.out.println("Dexterity: 0");
//                System.out.println("Constiution: 0");
//                System.out.println("Charisma: 0");
//                System.out.println("Wisdom: 0");
//                System.out.println(" Use 15, 14, 13, 12, 10, and 8 to apply to your"
//                        + " stats");
//                str = input.nextInt();
//                intelli = input.nextInt();
//                dex = input.nextInt();
//                consti = input.nextInt();
//                charisma = input.nextInt();
//                wis = input.nextInt();
//                System.out.println("You're stats are as follows: ");
//                System.out.println("Strength: " +getStr());
//                System.out.println("Intelligence: " + getIntelli());
//                System.out.println("Dexterity: " + getDex());
//                System.out.println("Constiution: " + getConsti());
//                System.out.println("Charisma: " + getCharisma());
//                System.out.println("Wisdom: " + getWis());
//                System.out.println("Do you want to continue(1) or redo(2)?");
//                if(input.nextInt() == 1) {
//                    break;
//                }
//            }
//
//        }
        public int checkModifier(int stat){
//p.10 of Handbook
//Checks if Ability Score causes a modifier
            if(stat == 6 || stat == 7){
                return -2;
            }
            else if(stat == 8 || stat == 9){
                return -1;
            }
            else if(stat == 10 || stat == 11){
                return 0;
            }
            else if(stat == 12 || stat == 13){
                return +1;
            }
            else if(stat == 14 || stat == 15){
                return +2;
            }
            else if(stat == 16 || stat == 17){
                return +3;
            }
            else if(stat == 18 || stat == 19){
                return +4;
            }
            return 0;
        }

        public void toWrite(){
            //Writes information about character object into a file with the character's name
//            File file = new File(getName() + ".txt");
//            try{
//                PrintWriter outPut = new PrintWriter(file);
//                outPut.println("Name: " + getName());
//                outPut.println("Class: " + getProfession());
//                outPut.println("Wealth: " + getWealth());
//                outPut.println("Hit points: " + getHP());
//                outPut.println("-------");
//                outPut.println("You're stats are as follows: ");
//                outPut.println("Strength: " +getStr());
//                outPut.println("Intelligence: " + getIntelli());
//                outPut.println("Dexterity: " + getDex());
//                outPut.println("Constiution: " + getConsti());
//                outPut.println("Charisma: " + getCharisma());
//                outPut.println("Wisdom: " + getWis());
//                outPut.close();
//            }catch(Exception e){
//                System.out.println("Error found");
//            }
        }

}