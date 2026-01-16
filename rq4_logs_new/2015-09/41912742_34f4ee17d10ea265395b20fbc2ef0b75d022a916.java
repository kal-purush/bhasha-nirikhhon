package filemanagement;

import java.io.File;   
  
public class RenameFile {  
	public void renameFile(String OldFilePath, String NewFilePath) {  
		File oldFileName = new File(OldFilePath);  
		File newFileName = new File(NewFilePath);  
		try {  
			if (oldFileName.renameTo(newFileName)) {  
				System.out.println("File renamed successfull !");  
			}
			else {  
				System.out.println("File rename operation failed !");  
			}  
		}
		catch (Exception e) {  
			e.printStackTrace();  
		}  
	}  
}  

