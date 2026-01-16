import java.util.Scanner;
import java.util.Random;

class Fighter{
    Pet pet;
    int hp;
    String heroName;
    int damage;
    String traits = "basic";

    // CONSTRUCTOR
    public Fighter(Pet pet,int hp, String heroName,int damage,String traits){
        this.pet = pet;
        this.hp = hp;
        this.heroName = heroName;
        this.damage = damage;
        this.traits = traits;
    }
    
    public Fighter(int hp, String heroName,int damage,String traits){
        this.hp = hp;
        this.heroName = heroName;
        this.damage = damage;
        this.traits = traits;

    }

    // ATTACKING BEHAVIOR
    public void attacked(Fighter attacker) {
        if (this.hp > 0) {
            if (attacker.pet != null) {
                this.hp -= attacker.damage + attacker.pet.damage;
                System.out.println("+---------------------------------------------+");
                System.out.printf("%s gets hit by %s and loses %d HP!\n", this.heroName, attacker.heroName, attacker.damage + attacker.pet.damage);
                System.out.printf("%s health reduce to %d !\n", this.heroName,this.hp);
                System.out.println("+----------------------------------------------+\n\n");
                if (this.hp <= 0){
                    System.out.println("========================");
                    System.out.println(this.heroName + " is dead");
                    System.out.println("========================");
                    this.Defeated();
                    System.out.println("------------------------");
                    attacker.cheers();
                    System.out.println("------------------------");
                    attacker.pet.cheers();
                    System.out.println("------------------------");
                }
            } else {
                this.hp -= attacker.damage;
                System.out.println("+---------------------------------------------+");
                System.out.printf("%s gets hit by %s and loses %d HP!\n", this.heroName, attacker.heroName, attacker.damage);
                System.out.printf("%s health reduce to %d !\n", this.heroName,this.hp);
                System.out.println("+---------------------------------------------+\n\n");
                if (this.hp <= 0){
                    System.out.println("========================");
                    System.out.println(this.heroName + " is dead");
                    System.out.println("========================");
                    this.Defeated();
                    System.out.println("------------------------");
                    attacker.cheers();
                    System.out.println("------------------------");
                }
            }
        } else {
            System.out.println(this.heroName + " is already dead");
        }
    }

    public void attacking(Fighter target){
       target.attacked(this);
    }

    public void Stats(){
        System.out.println("\n+-------"+this.heroName+" Stat-------+");
        System.out.println("Name: " + this.heroName);
        System.out.println("Health: " + this.hp);
        System.out.println("Strength: " + this.damage);
        if(pet!= null){
        System.out.println("Pet Name: " + this.pet.petName);}
        System.out.println("+-------------------------+\n");

    }

    public void cheers(){

        switch (this.traits) {
            case "basic":
                System.out.println(this.heroName + ": Victory is ours! Well fought, my friends!");
                break;

            case "cold":
                System.out.println(this.heroName + ": Another victory secured. Your efforts were insufficient.");
                break;    
    
            case "charming":
                System.out.println(this.heroName + ": A splendid performance, wouldn't you agree?");
                break;  
    
            case "wise":
                System.out.println(this.heroName + ": Strategy prevailed once again. Victory is ours.");
                break;  
    
            case "badass":
                System.out.println(this.heroName + ": Hah! Another one bites the dust. Who's next?");
                break;  

            case "courageous":
                System.out.println(this.heroName + ": Courage and valor. A glorious victory!");
                break;
            
            case "fierce":
                System.out.println(this.heroName + ": Tremble before our might! Victory is ours!");
                break;
    
            case "mysterious":
                System.out.println(this.heroName + ": Victory whispers secrets known only to the darkness.");
                break;
    
            case "honorable":
                System.out.println(this.heroName + ": Victory achieved with honor. We fought with honor.");
                break;
    
            case "reckless":
                System.out.println(this.heroName + ": Ha! Victory snatched from the jaws of chaos!");
                break;
    
            default:
                System.out.println(this.heroName + ": Victory is ours! Well fought, my friends!");
                break;
        }
    }
    
    public void Defeated() {
        switch (this.traits) {
            case "basic":
                System.out.println(this.heroName + ": This cannot be... Defeated...");
                break;
    
            case "cold":
                System.out.println(this.heroName + ": It seems... my end has come...");
                break;
    
            case "charming":
                System.out.println(this.heroName + ": Farewell... my dear companions...");
                break;
    
            case "wise":
                System.out.println(this.heroName + ": Alas... my strategies failed me...");
                break;
    
            case "badass":
                System.out.println(this.heroName + ": I never thought... I'd meet my match...");
                break;
    
            case "courageous":
                System.out.println(this.heroName + ": I fought... with all my courage... and yet...");
                break;
    
            case "fierce":
                System.out.println(this.heroName + ": My strength... was not enough...");
                break;
    
            case "mysterious":
                System.out.println(this.heroName + ": The darkness... it claims me...");
                break;
    
            case "honorable":
                System.out.println(this.heroName + ": My honor... remains intact...");
                break;
    
            case "reckless":
                System.out.println(this.heroName + ": My recklessness... led me to this end...");
                break;
    
            default:
                System.out.println(this.heroName + ": This cannot be... Defeated...");
                break;
        }
    }
    


    }



class Pet{
    int damage;
    String petName;

    public Pet(int damage,String petName){
        this.damage = damage;
        this.petName = petName;
    }

    public void cheers(){
        System.out.println(this.petName + ": Horrey >\\\\<");
    }

}





public class soal1 {
    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);
        Random rand = new Random();

        Pet Dragon = new Pet(10,"Night Fury");
        Fighter king = new Fighter(230, "King", 25 , "badass");
        Fighter armorKing = new Fighter(300, "ArmorKing", 15 , "reckless");
        Fighter kazuya = new Fighter(70, "Kazuya", 50 , "fierce");
        Fighter lily = new Fighter(100, "Lily", 25 , "charming");
        Fighter reina = new Fighter(230, "King", 25 , "courageous");
        Fighter bryan = new Fighter(150, "King", 30 , "reckless");
        Fighter dragunov = new Fighter( Dragon , 150, "Dragunov", 35 , "mysterious");

        try {
            System.out.println("     ===========   Welcome TO Tekken Gatcha    =============\n");
            System.out.println("\tits tekken but you use your luck power i guess\n");
            System.out.println("+-------------------CHOOSE YOUR CHARACTER-------------------+\n\n");
        
            System.out.println("\t---- 1. Dragunov ----\ncold-blooded killer\n\n\t----- 2. KING -----\nThe wrestling champion");
            System.out.println("\n\t---- 3. Armor King ----\nThe Raging Grappler\n\n\t----- 4. Kazuya -----\nThe Devil Incarnate");
            System.out.println("\n\t---- 5. Lily ----\nThe France Beauty\n\n\t----- 6. Reina -----\nThe Mysterios Mishima");
            System.out.println("\n\t---- 7. Bryan ----\nThe Cyborg Kickboxer\n\n\t----- 8. Custom Character -----\nCreate your own legend\n");
            System.out.print("\nType your choice: ");

            int userchoise = sc.nextInt();
            sc.nextLine();
            System.out.print("choose your opponent: ");
            int userchoise2 = sc.nextInt();
            sc.nextLine();
            Fighter[] fighters = {dragunov, king, armorKing, kazuya, lily, reina, bryan};
            Fighter myfighter = null;
            Fighter opponent = null;

        if(userchoise == 8){
            System.out.print("Enter the name of your character: ");
            String customName = sc.nextLine();
            System.out.print("Enter the HP of your character: ");
            int customHP = sc.nextInt();
            sc.nextLine(); 
            System.out.print("Enter the damage of your character: ");
            int customDamage = sc.nextInt();
            sc.nextLine(); 
            System.out.print("Enter the trait of your character: ");
            String customTrait = sc.nextLine();
            myfighter = new Fighter(customHP, customName, customDamage, customTrait);
            opponent = fighters[userchoise2-1];

        }
        else{
            
            myfighter = fighters[userchoise-1];
            opponent = fighters[userchoise2-1];
        }

        System.out.println("\n\n\t===== YOU CHOOSE "+ myfighter.heroName +" =====");
                myfighter.Stats();
                System.out.println("\t ==== Get ready to fight ====");
                System.out.println("\t        ==== "+opponent.heroName+" ====");

                while(opponent.hp > 0 && myfighter.hp > 0){
                    if (rand.nextInt(100) < 50){
                        myfighter.attacking(opponent);
                    }
                    else{
                        myfighter.attacked(opponent);
                    }

                }

    } catch (Exception e) {
        System.out.println("Invalid Input");
         return;
     }
        sc.close();
    }
}