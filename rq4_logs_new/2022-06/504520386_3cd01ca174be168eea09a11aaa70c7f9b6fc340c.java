import enums.SERVICE;
import enums.USER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataGenerator {

    public static void main(String[] args) {
        // pass each values in enums
        List a = product(SERVICE.values(), USER.values());
        System.out.println(a);
        System.out.println(a.get(1));
    }

    public static List<List<Enum>> product(Enum[]... enums) {
        return product(new ArrayList<>(Arrays.asList(enums)));
    }

    public static List<List<Enum>> product(List<Enum[]> enums) {
        if (enums.isEmpty()) {
            //Trivial case of recursive function
            return new ArrayList<>();
        }
        //remove first element
        Enum[] myEnums = enums.remove(0);
        List<List<Enum>> out = new ArrayList<>();
        for (Enum e : myEnums) {
            //call recursive
            List<List<Enum>> list = product(enums);
            for (List<Enum> list_enum : list) {
                //for each list get from recursion adding element e
                list_enum.add(0, e);
                out.add(list_enum);
            }
            if (list.isEmpty()) {
                List<Enum> list_enum = new ArrayList<>();
                list_enum.add(e);
                out.add(list_enum);
            }
        }
        enums.add(0, myEnums); //Backtraking
        return out;
    }

    public static void getInquireTestData(List<Enum[]> enums) {
//        SERVICE.ELECTRICITY,"customer1", "inquiry1"

    }

}