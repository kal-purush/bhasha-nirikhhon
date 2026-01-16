package dao;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.ibatis.session.SqlSession;
import org.springframework.stereotype.Repository;

import vo.PostVO;

@Repository
public class SearchDao implements IF_SearchDao {
	@Inject
	SqlSession sql;
	
	private static String mapperQuery = "dao.IF_SearchDao";

	//keyWord를 table에 저장
	@Override
	public void insertKeyWord(String keyWord) throws Exception {
		sql.insert(mapperQuery+".insertKeyWord", keyWord);
	}

	//keyWord를 가져오기
	@Override
	public List<String> selectKeyWord() throws Exception {
		return sql.selectList(mapperQuery+".selectKeyWord");
	}

	//검색 결과 가져오기
	@Override
	public List<PostVO> selectSearchList(Map map) throws Exception {
		return sql.selectList(mapperQuery+".selectSearchList", map);
	}

}