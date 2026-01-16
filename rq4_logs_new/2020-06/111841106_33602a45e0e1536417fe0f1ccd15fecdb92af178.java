public class main {

    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) {
        if (args.length != 1)
        {
            System.out.println("Usage: program <length>");
            return;
        }
        else
        {

            int length = Integer.parseInt(args[0]);
            System.out.println("Recursive:");
            printBinaryPermutationsRecursive("", length);
            System.out.println("Iterative:");
            printBinaryPermutationsIterative(length);
            System.out.println("Third Way:");
            aThird(length);
            System.out.println("Gab:");
            gab(length);
        }
    }

    public static void printBinaryPermutationsRecursive(String current, int iterations) {
        if (iterations == 0)
        {
            System.out.println(current);
        }
        else
        {
            printBinaryPermutationsRecursive(current + "0", iterations - 1);
            printBinaryPermutationsRecursive(current + "1", iterations - 1);
        }
    }

    public static void printBinaryPermutationsIterative(int length) {
        int values = (int)Math.pow(2, length);
        String format = "%" + length + "s";
        for (int i = 0; i < values; ++i)
        {
            System.out.println(String.format(format, Integer.toString(i, 2)).replace(" ", "0"));
        }

    }

    public static void aThird(int length)
    {
        for (int i = 0; i < Math.pow(2, length); i++)
        {
            String string = Integer.toBinaryString(i);
            for (int j = string.length(); j < length; j++)
            {
                string = "0" + string;
            }
            System.out.println(string);
        }
    }

    /*
     * Was looking at helping a friend with their HW
     * assignment while making these.  This is what they
     * came up with.  Stopped due to it being late.
     */
    public static void gab(int n)
    {
        int focus = 0;
        int container[] = new int[n];

        while(container[container.length - 1] != 1){
            for(int i = 0; i < container.length; i++){
                if(container[i] == 0){
                    container[i] = 1;
                    break;
                }else if(container[focus] == 1){
                    container[i] = 0;

                }
            }

            for(int j = 0; j < n; j++){
                System.out.print(container[j]);
            }
            System.out.print(" ");
        }
        System.out.println();
    }
}