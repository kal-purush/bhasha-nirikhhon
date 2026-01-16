import java.util.ArrayList;


public class  cpuScheduler {
	
	int timeslice=5;

	public static void roundRobin(ArrayList jobTable,int p[],int index){
				job job;
				job =(job) jobTable.get(index);
				int max=job.getMax();
				if (max>5){
					p[4]=5;
					job.setMax(max-=5);
				}
					else
						p[4]=max;
	}
}
			