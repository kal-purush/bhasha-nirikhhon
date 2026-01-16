package je.panse.doro.comm.item_execute.souslab7;

import java.util.Scanner;	

import je.panse.doro.comm.item_administratus.file.File_cdrw_proc;
import je.panse.doro.comm.item_administratus.key.Key_Iwbb;
import je.panse.doro.comm.item_administratus.loop.CurrentDateAdd_date;
import je.panse.doro.hito.newsub.New6OBJ;
import je.panse.doro.main.Enter;

public class CalcBP2 {
	int SBP, DBP, PR;
	String BPresult;
	public static void main(String[] args) throws Exception {
		File_cdrw_proc fcp1 = new File_cdrw_proc(); 
		Key_Iwbb bb1 = new Key_Iwbb();
		CalcBP2 bp1 = new CalcBP2();

		try (Scanner input = new Scanner(System.in)) {
			System.out.print("\r***** Input  [SBP ] [ DBP ]  [pulse rate / minute ] : ");
			bp1.SBP = input.nextInt();
			bp1.DBP = input.nextInt();
			bp1.PR = input.nextInt();
			//        input.close();
			bp1.BPresult = String.format("\tBP [ %d / %d ]mmHg   PR [ %d ]/min  Regular LSP", bp1.SBP,bp1.DBP,bp1.PR);
		   fcp1.writeliner(Enter.wts + "/6OBJ","\tat GDS clinic\t"+ CurrentDateAdd_date.main("h") +"\n");
		   fcp1.writeliner(Enter.wts + "/6OBJ", bp1.BPresult  + "\n");
		   fcp1.readfiler(Enter.wtss + "/6OBJ_List");
			New6OBJ.main(null);
		} catch (NumberFormatException e) {
		e.printStackTrace();
		}
	}
//---------------------------------------------------		
}