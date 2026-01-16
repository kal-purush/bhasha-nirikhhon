import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

public class GreedyAlgorithm {
    public static void main(String[] args) {
        System.out.println("Activitiy Selection (end time sorted): "+activity_selection_sorted(new int[]{1,3,0,5,8,5}, new int[]{2,4,6,7,9,9}));
        System.out.println("Activitiy Selection (unsorted): "+activity_selection_unsorted(new int[]{1,3,0,5,8,5}, new int[]{2,4,6,7,9,9}));
        System.out.println("Fractional Knapsack: "+fractional_knapsack(new float[]{60, 100, 120}, new float[]{10, 20, 30}, 50));
        System.out.println("Minimum Sum Absolute Difference: "+min_sum_absolute_difference_pairs(new int[]{4,1,8,7}, new int[]{2,3,6,5}));
        System.out.println("Maximum length chain of pairs: "+max_length_chain_of_pairs(new int[][]{{5,24},{39,60},{5,28},{27,40},{50,90}}));
        System.out.println("Minimum number of Indian Coins: "+min_indian_coins(590, new int[]{1,2,5,10,20,50,100,500,2000}));
        System.out.print("Job Sequencing Problem: "); job_sequencing(new int[][]{{4,20},{1,10},{1,40},{4,40},{1,30}});
        System.out.println("Chocola Problem: "+chocola_problem(new Integer[]{4,1,2}, new Integer[]{2,1,3,1,4}));

    }

    public static int activity_selection_sorted(int start[], int end[]){
        int count = 1; // A0 selected
        int prev_end = end[0];

        for(int i=1; i<start.length; i++){
            if(prev_end <= start[i]){
                count++;
                prev_end = end[i];
            }
        }
        return count;        
    }

    public static int activity_selection_unsorted(int start[], int end[]){
        //sort according to ending time
        int activities[][] = new int[start.length][3]; // index start end
        for(int i=0; i<start.length; i++){
            activities[i][0] = i; // original index (helps to know activity order after sorting)
            activities[i][1] = start[i]; // start
            activities[i][2] = end[i]; // end
        }

        // lambda function -> shortform
        // sort activities 2D arrays on basis of o[2] (end) as key
        Arrays.sort(activities, Comparator.comparingDouble(o -> o[2]));

        int count = 1; // A0 selected
        int prev_end = activities[0][2];
        
        for(int i=1; i<start.length; i++){
            if(prev_end <= activities[i][1]){
                count++;
                prev_end = activities[i][2];
            }
        }
        return count;        
    }

    // W is capacity
    public static float fractional_knapsack(float value[], float weight[], float W){
        float items[][] = new float[value.length][3];
        for(int i=0; i<value.length; i++){
            items[i][0] = value[i]; // value
            items[i][1] = weight[i]; // weight
            items[i][2] = value[i]/weight[i]; // price => value/weight
        }

        Arrays.sort(items, Comparator.comparingDouble(o -> o[2]));
        
        float max_val = 0;
        int index = 0;
        for(index=value.length-1; index>=0; index--){
            if( W >= items[index][1]){
                max_val += items[index][0]; // value
                W = W - items[index][1]; // weight
            }else {
                max_val += items[index][2] * W;
                W = 0; // at last we take fraction of last element
                break;
            }
        }
        return max_val;
    }

    public static int min_sum_absolute_difference_pairs(int array_A[], int array_B[]){
        int sum = 0;
        Arrays.sort(array_A);
        Arrays.sort(array_B);

        for(int i=0; i<array_A.length; i++){
            sum += Math.abs(array_A[i]-array_B[i]);
        }

        return sum;
    }

    public static int max_length_chain_of_pairs(int pairs[][]){
        Arrays.sort(pairs, Comparator.comparingDouble(o -> o[1])); // sort by second number
        int count = 1;
        int prev_second_num = pairs[0][1];

        for(int i=1; i<pairs.length; i++){
            if(pairs[i][0] >= prev_second_num){
                count++;
                prev_second_num = pairs[i][1];
            }
        }
        return count;
    }

    public static int min_indian_coins(int value, int denominations[]){
        Arrays.sort(denominations);
        int count = 0;
        int index = denominations.length-1;
        while(value!=0){
            while( value < denominations[index] ){
                index--;
            }
            count++;
            value = value - denominations[index];
            // System.out.print(denominations[index] + " ");
        }
        return count;
    }

    public static void job_sequencing(int jobs[][]){
        ArrayList<Job> list = new ArrayList<>();
        for(int i=0; i<jobs.length; i++){            
            list.add(new Job(i, jobs[i][0], jobs[i][1]));
        }
        Collections.sort(list, (obj1, obj2) -> obj2.profit - obj1.profit);
        
        // ArrayList<Job> ans = new ArrayList<>();
        int time = 0;
        for(int i=0; i<jobs.length; i++){
            Job curr = list.get(i);
            if(curr.deadline > time){
                System.out.print("J"+curr.id+" ");
                time++;
            }    
        }
        System.out.println();
    }

    public static int chocola_problem(Integer horizontal[], Integer vertical[]){
        int min_cost = 0;
        Arrays.sort(horizontal, Collections.reverseOrder());
        Arrays.sort(vertical, Collections.reverseOrder());

        int ver_ptr = 0;
        int hor_ptr = 0;
        while(ver_ptr != vertical.length && hor_ptr != horizontal.length){
            if(vertical[ver_ptr] >= horizontal[hor_ptr]){
                min_cost += vertical[ver_ptr] * (hor_ptr+1); // cost of vertical * number of horizontal cuts
                ver_ptr++;
            }else{
                min_cost += horizontal[hor_ptr] * (ver_ptr+1); // cost of horizontal * number of vertical cuts
                hor_ptr++;
            }
        }
        while(ver_ptr != vertical.length){
            min_cost += vertical[ver_ptr] * (hor_ptr+1); // cost of vertical * number of horizontal cuts
            ver_ptr++;
        }
        while(hor_ptr != horizontal.length){
            min_cost += horizontal[hor_ptr] * (ver_ptr+1); // cost of vertical * number of horizontal cuts
            hor_ptr++;
        }
        return min_cost;
    }
}

class Job {
    int id;
    int deadline;
    int profit;

    public Job(int id, int deadline, int profit){
        this.id = id;
        this.deadline = deadline;
        this.profit = profit;
    }

    @Override
    public String toString() {
        return "("+id+", "+deadline+", "+profit+")";
    }
}