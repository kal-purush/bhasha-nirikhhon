import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import ithakimodem.Modem;

public class Ack {


	public boolean ackError(Modem modem,String command, boolean ackSent){

		int k;
		PrintWriter ackResponse = null;
		int runtime = 240;
		try {
			 String fileName = new SimpleDateFormat("yyyy-MM-dd hh-mm-ss'.txt'").format(new Date());

			ackResponse= new PrintWriter(fileName,"UTF-8");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long tic=0,toc=0;
		String field,xor;
		String[] fieldMatrix;
		String packet="";
		int totalPackets =0;
		int correctPackets =0;
		int errorPackets = 0;
		int assumption[] = new int[10];
		long stop=System.nanoTime()+TimeUnit.SECONDS.toNanos(runtime);
		String nack_command = "R4898\r";
		while ((k=modem.read())!=-1){
			System.out.print((char)k);
		}
		tic = System.currentTimeMillis();
		modem.write(command.getBytes());
		int errorIndex = 0;
		while ((k=modem.read())!=-1){
			packet+=(char)k;
			if (packet.endsWith("PSTOP")){
				field = packet.substring(31,47);
				xor = packet.substring(49,52).replaceAll("\\s+","");
				int xxor = Integer.parseInt(xor);
				char xored;
				fieldMatrix = field.split("");
				char xor2 = 0;
				int i=0,j=0;
				xored=fieldMatrix[0].charAt(0);
				for (i=0;i<15;i++){
					j=i+1;
					xor2=fieldMatrix[j].charAt(0);
					xored = (char) (xored^xor2);
				}
				if ((int)xored == xxor){
					toc = System.currentTimeMillis();
					float responseTime = (float) ((float)(toc-tic)/1000);
						//System.out.println("Time elapsed: "+responseTime);
						ackResponse.write(Float.toString(responseTime));
						ackResponse.write(System.lineSeparator());
						tic = System.currentTimeMillis();
						correctPackets++;
						//PACKET SENT IS OK, SEND NEXT PACKET.
						//System.out.println("Successfully sent packet");
						assumption[errorIndex]++;
						errorIndex=0;
						modem.write(command.getBytes());
				}
				else{
					//System.out.println("Sending NACK command");
					modem.write(nack_command.getBytes());
					errorPackets++;
					errorIndex++;
				}
			totalPackets++;
			packet="";
			}
		//System.out.println(stop-System.nanoTime());
		if (stop<=System.nanoTime())break;
		}
		ackResponse.close();
		System.out.println("Totalpackets: "+totalPackets);
		System.out.println("Correct packets: "+correctPackets);
		System.out.println("Resent packets: "+errorPackets);
		for (int i =0;i<assumption.length;i++){
			System.out.print(assumption[i]+" " );
		}
		return true;
	}
}
