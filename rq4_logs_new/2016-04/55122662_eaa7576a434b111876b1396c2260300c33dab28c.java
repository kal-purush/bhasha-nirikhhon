package uriage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import uriageEntity.Branch;
import uriageEntity.Commodity;

public class Calculate {

	/**
	 * 売上ファイルを読み込み、エンティティにセットする。
	 **/
	public void calculate(List<Branch> braList, List<Commodity> comList, List<String> rcdList) {

		//支店別に集計
		for (String rcd : rcdList) {

        	try {
		        //ファイルを読み込む
		        FileReader fr = new FileReader(rcd);
		        BufferedReader br = new BufferedReader(fr);

		        //1行ずつ読みこんで格納
		        String line;

		        while ((line = br.readLine()) != null) {

		        	String[] cols = line.split(",", -1);
	        		//Branchエンティティに格納
	        		Branch bra = new Branch();
	        		// cols[0];
	        		bra.setBranchName(cols[1]);
	        		//String price = col[2];

	        		braList.add(bra);

		        }

		        //終了処理
		        br.close();
		        fr.close();

		    } catch (IOException ex) {
		        //例外発生時処理
		        System.out.println(ex);
		    }

		}

		//商品別に集計

	}

}