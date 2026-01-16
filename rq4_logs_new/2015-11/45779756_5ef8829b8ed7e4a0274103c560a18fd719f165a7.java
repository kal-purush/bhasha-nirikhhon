package codingtests.test5_listrotate;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class ListRotate {
    public List<Character> rotate(int nr, List<Character> charList) {
        Preconditions.checkArgument(charList.size() > nr, "List should be longer than the chosen rotation number.");

        if(nr > 0) {
            List<Character> newList = new ArrayList<>(charList.subList(nr,charList.size()));
            newList.addAll(charList.subList(0,nr));
            return newList;
        }

        if(nr < 0) {
            List<Character> newList = new ArrayList<>(charList.subList(charList.size()+nr,charList.size()));
            newList.addAll(charList.subList(0,charList.size()+nr));
            return newList;
        }

        return charList;
    }
}