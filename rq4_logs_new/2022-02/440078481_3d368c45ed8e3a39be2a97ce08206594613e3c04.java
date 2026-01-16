package MergeIntervals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConflictingAppointments {
    public ConflictingAppointments(){
        List<int[]> appointments = new ArrayList<>();
        appointments.add(new int[]{6, 7});
        appointments.add(new int[]{2, 4});
        appointments.add(new int[]{8, 12});

        System.out.println(this.checkforConflicts(appointments));
    }

    private boolean checkforConflicts(List<int[]> appointments){
        Set<Integer> set = new HashSet<>();

        for(int[] appointment: appointments){
            int i = appointment[0] + 1;

            while(i <= appointment[1]){
                if(set.contains(i)){

                    return false;
                }
                set.add(i);
                i++;
            }
        }

        return true;
    }
}