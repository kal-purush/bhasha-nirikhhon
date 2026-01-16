package com.island.app.community.qna.dao;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.RowBounds;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

import com.island.app.common.page.PageVo;
import com.island.app.community.qna.vo.QnaVo;
/**
 *
 * @author 김수진
 *
 */
@Repository
public class QnaDao {
	
	//목록조회
	public List<QnaVo> getQnaList(SqlSessionTemplate sst, PageVo pv, Map<String , String> searchMap) {
		
		int limit = pv.getBoardLimit();
		int offset = (pv.getCurrentPage()-1) * limit;
		RowBounds rb = new RowBounds(offset , limit);
		return sst.selectList("qna.getQnaList" , searchMap , rb);
	}
	
	//작성하기
	public int write(SqlSessionTemplate sst, QnaVo vo) {
		return sst.insert("qna.write" , vo);
	}
	
	//상세조회(조회수)
	public QnaVo getQna(SqlSessionTemplate sst, String no) {
		return sst.selectOne("qna.getQna",no);
	}
	
	//조회수
	public int increaseHit(SqlSessionTemplate sst, String no) {
		return sst.update("qna.increaseHit",no);
	}
	
	//수정하기 화면
	public QnaVo getQnaEdit(SqlSessionTemplate sst, String no) {
		return sst.selectOne("qna.getQnaEdit",no);
	}
	
	//수정하기(작성자만)
	public int updateQna(SqlSessionTemplate sst, QnaVo vo) {
		return sst.update("qna.updateQna",vo);
	}
	
	//삭제하기(작성자만)
	public int delete(SqlSessionTemplate sst, QnaVo vo) {
		return sst.update("qna.delete",vo);
	}
	
	//게시글 갯수조회
	public int getCnt(SqlSessionTemplate sst, Map<String, String> searchMap) {
		return sst.selectOne("qna.getCnt",searchMap);
	}

	public List<Map<String, String>> getCategoryList(SqlSessionTemplate sst) {
		return sst.selectList("qna.getCategoryList");
	}


}