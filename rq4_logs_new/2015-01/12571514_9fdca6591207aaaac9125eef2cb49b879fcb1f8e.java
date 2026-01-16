package efseq;

import java.io.IOException;

/**
 * Prepares paired end fastq files for initial alignment and subsequent DCS processing.
 * 
 * @author Lisle Mose
 */
public class FastqPreprocessor2 {
	
	public static final int TAG_LENGTH =  14;
	
	/**
	 * Prep input FASTQ files for initial alignment and downstream consensus sequence processing. 
	 */
	public void preProcess(String fastqInput1, String fastqInput2, 
			String fastqOutput1, String fastqOutput2) throws IOException {
		FastqInputFile fastq1 = new FastqInputFile();
		FastqInputFile fastq2 = new FastqInputFile();
		
		FastqOutputFile out1 = new FastqOutputFile();
		FastqOutputFile out2 = new FastqOutputFile();
		
		fastq1.init(fastqInput1);
		fastq2.init(fastqInput2);
		
		out1.init(fastqOutput1);
		out2.init(fastqOutput2);
		
		FastqRecord rec1 = fastq1.getNextRecord();
		FastqRecord rec2 = fastq2.getNextRecord();
		
		int cnt = 1;
		
		while ((rec1 != null) && (rec2 != null)) {
			
			String tag1 = rec1.getId().substring(rec1.getId().length()-TAG_LENGTH, rec1.getId().length());
			String tag2 = rec2.getId().substring(rec2.getId().length()-TAG_LENGTH, rec2.getId().length());

			rec1.setId("@" + tag1 + "_" + rec1.getId());
			rec2.setId("@" + tag2 + "_" + rec2.getId());
			
			out1.write(rec1);
			out2.write(rec2);
			
			rec1 = fastq1.getNextRecord();
			rec2 = fastq2.getNextRecord();
			
			if ((cnt++ % 1000000) == 0) {
				System.err.println("Processed " + (cnt-1) + " read pairs.");
			}
		}
		
		out1.close();
		out2.close();
		fastq1.close();
		fastq2.close();
		
		System.err.println("Done.");
	}
		
	
	public static void main(String[] args) throws Exception  {
		FastqPreprocessor2 prep2 = new FastqPreprocessor2();
		prep2.preProcess("/home/lmose/code/efseq/test/test1.fastq", "/home/lmose/code/efseq/test/test2.fastq",
				"/home/lmose/code/efseq/test/out1.fastq", "/home/lmose/code/efseq/test/out2.fastq");
	}
}