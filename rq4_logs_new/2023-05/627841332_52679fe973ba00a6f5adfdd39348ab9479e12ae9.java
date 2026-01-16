package com.island.app.admin.notice.dao;

import java.util.List;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

import com.island.app.admin.notice.vo.NoticeVo;

@Repository
public class NoticeDao {

	public List<NoticeVo> getNoticeList(SqlSessionTemplate sst) {
		return sst.selectList("adminnotice.getNoticeList");
	}

	public int write(SqlSessionTemplate sst, NoticeVo vo) {
		return sst.insert("adminnotice.adminNoticeWrite" , vo);
	}

	public NoticeVo getNotice(SqlSessionTemplate sst, String num) {
		return sst.selectOne("adminnotice.getNotice" , num);
	}

	public int edit(SqlSessionTemplate sst, NoticeVo vo) {
		return sst.update("adminnotice.edit" , vo);
	}

	public int delete(SqlSessionTemplate sst, String num) {
		return sst.delete("adminnotice.delete" , num);
	}

}