import java.text.SimpleDateFormat;
import java.util.Date;

import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;


public class FileLinkListener implements Listener {
		public Object parent;
		public String key;
		public String filePath;
		public String fileName;
		public Link link;
		
		public FileLinkListener(Object parent, String key, String filePath, Link link) {
			this.link = link;
			this.key = key;
			this.parent = parent;
			this.filePath = filePath;
			this.fileName = filePath.replaceAll(".*\\\\", "");
		}
		
		public void handleEvent(Event event) {
			
			/*
			 SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yy");
			String start = "01/01/1930";
			sdf.set2DigitYearStart(new Date(start));
			System.out.println("START:  " + start);
			for (int i = 0; i < 99; i += 5) {
				try {
					System.out.println(String.format("%2d  -->  %s", i, sdf.parse("1/1/" + String.format("%02d",i)).toString()));
				} catch (Exception e) {
				}
			}
			
			
			if (false) 
				return;
			*/
			
			// Open the link's file
			ReadmeUI.openFile(filePath, key);
			
			// If it's a config file, and the corresponding output file exists... open that as well.
			boolean bLinkIsConfig = key.equalsIgnoreCase("Configuration");
			if (bLinkIsConfig) {
				String outputFile = Utils.getFileDirectory(Prefs.getOutputPath()) + "\\" + Utils.getFileName(filePath).replaceFirst("Config_", "Readme_");
				boolean bOpenOutputFile = Utils.fileExists(outputFile);
				if (bOpenOutputFile) {
					ReadmeUI.openFile(outputFile, "Output");
				}
			}
			
			// Update the UI
			link.getParent().layout();
			ReadmeUI.updateActionInfo();
			ReadmeUI.log(key + ": click on " + event.text);
		}
	}