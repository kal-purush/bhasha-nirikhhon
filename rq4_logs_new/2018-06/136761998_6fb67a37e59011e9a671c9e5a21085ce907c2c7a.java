public class MultiplicationTable_v2 {
    public static void main (String [] args){
        for(int i = 0; i <= 12; i++) {
            if(i < 10) {
                System.out.print(i + "  : ");
            } else {
                System.out.print(i + " : ");
            }
        }

        System.out.println();

        for(int i = 0; i <= 12; i++) {
            if(i < 10) {
                System.out.print("-----");
            } else {
                System.out.print("----");
            }
        }

        System.out.println();

        for(int i=1; i <= 12; i++) {
            if(i<10) {
                System.out.print(i + "  | ");
            } else {
                System.out.print(i + " | ");
            }
            for (int j=1; j <= 12; j++) {
                if(i*j < 10) {
                    System.out.print(i * j + "  : ");
                } else if(i*j < 100) {
                    System.out.print(i * j + " : ");
                } else {
                    System.out.print(i * j + ": ");
                }
            }
            System.out.println();
        }
    }
}