package iotwearable.gen;

import java.util.ArrayList;
import java.util.List;

public class CodeSpliter {
	private String sourceCode;
	private static String  EXTERN ="extern ";
	public CodeSpliter(String sourceCode) {
		this.sourceCode = sourceCode;
	}
	public List<String> splitFile() {
		List<String> list = new ArrayList<>();
		List<String> listConfig  = new ArrayList<>();
		StringBuilder builderApp = new StringBuilder();
		String[] arraySource = sourceCode.split("\n");
		boolean isConfig = true;
		for( int  i=0;i< arraySource.length ;i ++) {
			if(!arraySource[i].equals("/*--------------------END Define--------------------*/") && isConfig) {
				listConfig.add(arraySource[i]);
			}
			else if(arraySource[i].equals("/*--------------------END Define--------------------*/")) {
				isConfig = false;
			}
			else if(!isConfig) {
				builderApp.append(arraySource[i]+"\n");
			}
		}
			
		list.add(createConfig(listConfig));
		list.add("#include \"config.h\" \n" + builderApp.toString());
		return list;

	}
	private String createConfig(List<String> configSoucre) {
		StringBuilder builder = new StringBuilder();
		builder.append("#ifndef CONFIG   /* Include guard */ \n");
		builder.append("#define CONFIG \n");
		for(String text: configSoucre) {
			builder.append(exportCode(text));
		}
		builder.append("#endif // FOO_H_\n");
		return builder.toString();
	}
	private String exportCode(String code) {
		StringBuilder builder = new StringBuilder();
		builder.append(code + "\n");
		if(code.contains("=")) {
			builder.append(EXTERN + code.split("=")[0]+ ";\n");
		}
		else if(code.contains(":")){
			builder.append(EXTERN + code.split(":")[0]+ ";\n");
		}
		else if(code.contains("#define")) {
			builder.append(EXTERN + code.substring(0, code.lastIndexOf(" ")) +"\n");
		}
		else if(code.contains("(RX,TX)")) {
			builder.append(EXTERN + code +"\n");
		}
		return builder.toString();
	}
}