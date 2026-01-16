package je.panse.doro.comm.item_execute.souslab7.Proc_CalcBP;

import java.util.ArrayList;
import java.util.Scanner;

public class Bp_ArrayListInput {
	public static ArrayList<String> getStrings() {
		ArrayList<String> inputs = new ArrayList<>();
		Scanner scan = new Scanner(System.in);
    while (true) {
      System.out.println("Enter multiple inputs, press Enter to stop:");
      String input = scan.nextLine();
      if (input.isEmpty()) {
        break;
      }
      inputs.add(input);
    }
    scan.close();
    return inputs;
  }
}