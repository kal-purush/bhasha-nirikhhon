package com.project.reply.dao.impl;

import com.project.reply.dao.ReplyDao;
import com.project.reply.vo.ReplyVo;
import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ReplyDaoImpl implements ReplyDao {

    @Autowired
    SqlSession sqlSession;

    public List<ReplyVo> getReplylist(int board_number){
        List<ReplyVo> readReply = sqlSession.selectList("Reply.Replylist", board_number);
        return readReply;
    }

    @Override
    public void writeReply(ReplyVo replyVo) {
        sqlSession.insert("Reply.WriteReply", replyVo);
    }
}