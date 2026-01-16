import java.io.*;
import java.util.*;

import java.net.URL;
import java.net.HttpURLConnection;

import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class CrawlCodechef{
	private String  codechefURL = "https://www.codechef.com"; 
	private String username;

	private List<String> problemURLs;
	private List<String> problemNames;
	private List<String> solutionIDs;
	private List<String> language;

	private HttpURLConnection conn;

	public CrawlCodechef(String username_){
		this.username = username_;	
		this.problemURLs = new ArrayList<>();
		this.problemNames = new ArrayList<>();
		this.solutionIDs = new ArrayList<>();
		this.language = new ArrayList<>();
	}

	public void Crawl(){
		//-------------Find Successful problems-----------------------
		String content_codechef = getPageContent(codechefURL+"/users/"+username);
		Document doc = Jsoup.parse(content_codechef);

		Elements categories = doc.select("td");
		for(int i=0;i<categories.size();i++){
			String heading = categories.get(i).text();
			if(heading =="Problems Successfully Solved"){
				Elements problems = categories.get(i+1).select("a");
				for(Element problem : problems)
				{
					problemNames.add(problem.text());
					problemURLs.add(problem.attr("href"));
				}
			}
		}


		//--------------Get Submission IDs of successful problems----
		for(int i=0;i<problemURLs.size();i++){
			String url = codechefURL+problemURLs.get(i);
			Document doc = Jsoup.parse(getPageContent(url));

			Elements rows = doc.select("tr[class=\\\"kol\\\"]");
			for (Element row:rows){
				Elements fields = row.select("td");
				// if the solution is hidden then break
				if(fields.toString().contains("No Recent Activity"));
					break;

				//finding the row with accepted solution
				if(fields.get(3).toString().contains("tick-icon.gif")){
					solutionIDs.add(fields.get(0).text());
					language.add(fields.get(6).text());
					break;
				}
			}
		}


		//-----------------Download solution---------------
		String initial_url = codechefURL+"/viewplaintext/";

		for(int i=0;i<solutionIDs.size();i++){
			String url = initial_url + solutionIDs.get(i);
			String content = getPageContent(url);
			String code = Jsoup.parse(content).select("pre").text();

			String filename = problemNames.get(i)+file_extension(language.get(i));
			File file = new File("Codechef_solutions");
			FileWriter fileWriter = new FileWriter(file);

			StringTokenizer tokens = new StringTokenizer(code,"\n");
			while(tokens.hasMoreTokens())
				fileWriter.write(tokens.nextToken()+"\n");

			fileWriter.close();
		}
		
		System.out.println("Download Completed !!!");	
	}

	//------------------Making connection and Content Extraction---------------------
	private String getPageContent(String url){
		URL obj = new URL(url);
		conn = (HttpURLConnection) obj.openConnection();
		conn.setRequestMethod("GET");
		conn.setUseCaches(false); 

		BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream())); 
		String response = new String();
		String inputLine;

		while(inputLine=in.readLine()!=null){
			response += inputLine;
		}
		in.close();

		return response;
	}

	//---------------Return file extension-----------------
	private String file_extension(String lang){
		if(lang.contains("JAVA"))
			return ".java";
		else if(lang.contains("C++"))
			return ".cpp";
		else if(lang.contains("PY"))
			return ".py";
		else if(lang.contains("C"))
			return ".c";
		else 
			return ".txt";
	}
}













