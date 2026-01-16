/**
 * 
 */
package org.howsun.sample.lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * 说明:
 * 
 * @author howsun ->[howsun.zhang@gmail.com]
 * @version 1.0
 *
 * 2017年3月27日 下午2:33:37
 */
public class LuceneIndexSample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			
			Directory d = FSDirectory.open(new File("e:/repository_lucene").toPath());
			
			Analyzer analyzer = new IKAnalyzer(true);
			IndexWriterConfig conf = new IndexWriterConfig(analyzer);
			conf.setOpenMode(OpenMode.CREATE_OR_APPEND);
			
			IndexWriter indexWriter = new IndexWriter(d, conf);
			indexWriter.deleteAll();
			
			Random random = new Random();

			URL url = LuceneIndexSample.class.getClassLoader().getResource("text");
			File file = new File(url.getFile());
			File textFiles[] = file.listFiles();
			for(File textFile : textFiles){
				String title = '《' + textFile.getName().replace(".txt", "") + '》';
				String content = getContent(textFile);
				addDoc(title, content, random.nextInt(100), 1 + random.nextInt(2), indexWriter);
				System.out.println(title);
			}
			
			indexWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void addDoc(String title, String content, int hit, int categoryId, IndexWriter indexWriter) throws IOException{
		Document document = new Document();

		TextField titleTextField = new TextField("title", title, Store.YES);
		titleTextField.setBoost(1.0f);
		document.add(titleTextField);

		TextField contentTextField = new TextField("content", content, Store.YES);
		contentTextField.setBoost(0.9f);
		document.add(contentTextField);

		IntField hitsTextField = new IntField("views", hit, Store.YES);
		document.add(hitsTextField);
		
		IntField catgoryTextField = new IntField("catgory", categoryId, Store.YES);
		document.add(catgoryTextField);
		
		indexWriter.addDocument(document);
	}

	private static String getContent(File file){
		InputStream inputStream = null;
		BufferedReader bufferedReader = null;
		try {
			InputStreamReader read = new InputStreamReader(new FileInputStream(file),"utf-8");//考虑到编码格式
			bufferedReader = new BufferedReader(read);
			String lineTxt = null;
			StringBuffer content = new StringBuffer();
			while((lineTxt = bufferedReader.readLine()) != null){
				content.append(lineTxt);
			}
			return content.toString();
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(bufferedReader != null){
				try {
					bufferedReader.close();	
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
			if(inputStream != null){
				try {
					inputStream.close();	
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
			
		}

		return null;
	}

}