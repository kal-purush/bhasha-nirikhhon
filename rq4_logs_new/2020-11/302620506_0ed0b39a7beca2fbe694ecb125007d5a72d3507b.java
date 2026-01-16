import java.util.*;

public class Lodky {
    public static int pok = 10;
    public static int um = 10;
    public static int mLode;
    public static int eLode;
    public static String[][] mr = new String[pok][um];
    public static int[][] missedGuesses = new int[pok][um];
    private static Lodky lodky;

    public static void main(String[] args){
       vytvorenieMapy();
        
        mojeLode();
        
        enemyLode();
        
        do {
            Bitva();
        }while(lodky.mLode != 0 && lodky.eLode != 0);


        vysledokBitvy();
    }

    public static void vytvorenieMapy(){
        System.out.print("  ");
        for(int i = 0; i < um; i++)
            System.out.print(i);
        System.out.println();
        
        for(int i = 0; i < mr.length; i++) {
            for (int j = 0; j < mr[i].length; j++) {
                mr[i][j] = " ";
                if (j == 0)
                    System.out.print(i + "|" + mr[i][j]);
                else if (j == mr[i].length - 1)
                    System.out.print(mr[i][j] + "|" + i);
                else
                    System.out.print(mr[i][j]);
            }
            System.out.println();
        }
        
        System.out.print("  ");
        for(int i = 0; i < um; i++)
            System.out.print(i);
        System.out.println();
    }

    public static void mojeLode(){
        Scanner input = new Scanner(System.in);

        System.out.println("\nна войну:");
        lodky.mLode = 5;
        for (int i = 1; i <= lodky.mLode; ) {
            System.out.print("Hodnota" + i + " z prava: ");
            int x = input.nextInt();
            System.out.print("Hodnota" + i + " z lava: ");
            int y = input.nextInt();

            if((x >= 0 && x < pok) && (y >= 0 && y < um) && (mr[x][y] == " "))
            {
                mr[x][y] =   "@";
                i++;
            }
            else if((x >= 0 && x < pok) && (y >= 0 && y < um) && mr[x][y] == "@")
                System.out.println("Nie na to iste miesto...");
            else if((x < 0 || x >= pok) || (y < 0 || y >= um))
                System.out.println("Nemozes ist mimo " + pok + um + " bojisko");
        }
        taMapa();
    }

    public static void enemyLode(){
        System.out.println("\nАмериканский лед в движении");
        lodky.eLode = 5;
        for (int i = 1; i <= lodky.eLode; ) {
            int x = (int)(Math.random() * 10);
            int y = (int)(Math.random() * 10);

            if((x >= 0 && x < pok) && (y >= 0 && y < um) && (mr[x][y] == " "))
            {
                mr[x][y] = "x";
                System.out.println(i + ". lode na miestach");
                i++;
            }
        }
        taMapa();
    }

    public static void Bitva(){
        mojUtok();
        ichUtok();

        taMapa();

        System.out.println();
        System.out.println("Наши корабли: " + lodky.mLode + " Американские капиталистические корабли: " + lodky.eLode);
        System.out.println();
    }

    public static void mojUtok(){
        System.out.println("\nВаша очередь");
        int x = -1, o = -1;
        do {
            Scanner in = new Scanner(System.in);
            System.out.print("Vloz poziciu zo boku: ");
            x = in.nextInt();
            System.out.print("Vloz poziciu z hora: ");
            o = in.nextInt();

            if ((x >= 0 && x < pok) && (o >= 0 && o < um))
            {
                if (mr[x][o] == "x")
                {
                    System.out.println("Zasah");
                    mr[x][o] = "!";
                    --lodky.eLode;
                }
                else if (mr[x][o] == " ") {
                    System.out.println("Oi netrafil si sa brasko");
                    mr[x][o] = "-";
                }
            }
            else if ((x < 0 || x >= pok) || (o < 0 || o >= um))
                System.out.println("Nemozes ist mimo " + pok + " by " + um + " bojisko");
        }while((x < 0 || x >= pok) || (o < 0 || o >= um));
    }

    public static void ichUtok(){
        System.out.println("\nАмериканское возвращение");
        int x = -1, y = -1;
        do {
            x = (int)(Math.random() * 10);
            y = (int)(Math.random() * 10);

            if ((x >= 0 && x < pok) && (y >= 0 && y < um))
            {
                if (mr[x][y] == "@")
                {
                    System.out.println("Potopil nam lod kapitane");
                    mr[x][y] = "x";
                    --lodky.mLode;
                    ++lodky.eLode;
                }
                else if (mr[x][y] == "x") {
                    System.out.println("Kapitalisti si potopili lod");
                    mr[x][y] = "!";
                }
                else if (mr[x][y] == " ") {
                    System.out.println("Kapitalisti netrafili");
                    if(missedGuesses[x][y] != 1)
                        missedGuesses[x][y] = 1;
                }
            }
        }while((x < 0 || x >= pok) || (y < 0 || y >= um));
    }

    public static void vysledokBitvy(){
        System.out.println("Советский союз: " + lodky.mLode + " | Capitalists: " + lodky.eLode);
        if(lodky.mLode > 0 && lodky.eLode <= 0)
            System.out.println("Мы выиграли :");
        else
            System.out.println("Damn capitalists");
        System.out.println();
    }

    public static void taMapa(){
        System.out.println();
        System.out.print("  ");
        for(int i = 0; i < um; i++)
            System.out.print(i);
        System.out.println();
        
        for(int x = 0; x < mr.length; x++) {
            System.out.print(x + "|");

            for (int y = 0; y < mr[x].length; y++){
                System.out.print(mr[x][y]);
            }

            System.out.println("|" + x);
        }
        
        System.out.print("  ");
        for(int i = 0; i < um; i++)
            System.out.print(i);
        System.out.println();
    }
}