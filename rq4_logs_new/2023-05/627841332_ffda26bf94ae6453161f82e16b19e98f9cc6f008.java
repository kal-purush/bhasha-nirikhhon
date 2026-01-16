package com.island.app.community.notice.dao;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.session.RowBounds;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

import com.island.app.admin.notice.vo.NoticeVo;
import com.island.app.common.page.PageVo;

@Repository
public class MemberNoticeDao {
	//공지사항 목록
	public List<NoticeVo> getNoticeList(SqlSessionTemplate sst, PageVo pv, Map<String, String> searchMap) {
		
		int limit = pv.getBoardLimit();
		int offset = (pv.getCurrentPage()-1) * limit;
		RowBounds rb = new RowBounds(offset , limit);
		return sst.selectList("notice.getNoticeList",searchMap, rb);
	}
	//공지사항 목록조회
	public int getCnt(SqlSessionTemplate sst, Map<String, String> searchMap) {
		return sst.selectOne("notice.getCnt",searchMap);
	}
	//공지사항 상세조회
	public NoticeVo getNotice(SqlSessionTemplate sst, String no) {
		return sst.selectOne("notice.getNotice",no);
	}

}