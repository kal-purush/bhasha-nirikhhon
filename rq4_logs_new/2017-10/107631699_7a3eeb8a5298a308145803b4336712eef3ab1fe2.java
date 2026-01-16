package cn.edu.nju.cs.itrace4.demo.algo;

import java.util.Comparator;
import java.util.Map;

import cn.edu.nju.cs.itrace4.core.document.SimilarityMatrix;

public class SortVertexByScore implements Comparator<Integer>{

	private Map<Integer, String> vertexIdNameMap;
	SimilarityMatrix matrix;
	String req;//requirement
	
	public SortVertexByScore(Map<Integer, String> vertexIdNameMap,SimilarityMatrix matrix,
			String req) {
		this.vertexIdNameMap = vertexIdNameMap;
		this.matrix = matrix;
		this.req = req;
	}
	
	@Override
	public int compare(Integer o1, Integer o2) {
		String className1 = vertexIdNameMap.get(o1);
		String className2 = vertexIdNameMap.get(o2);
		double score1 = matrix.getScoreForLink(req, className1);
		double score2 = matrix.getScoreForLink(req, className2);
		if(score2>score1) {
			return 1;
		}
		else if(score2<score1) {
			return -1;
		}
		else {
			return 0;
		}
	}

}