package persistence.DAO;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import persistence.DTO.CommentsDTO;
import persistence.DTO.MemberDTO;
import persistence.DTO.RecipeDTO;
import persistence.mapper.CommentsMapper;
import persistence.mapper.MemberMapper;
import persistence.mapper.RecipeMapper;

import java.util.List;

public class CommentsDAO {
        private SqlSessionFactory sqlSessionFactory = null;

        public CommentsDAO(SqlSessionFactory sqlSessionFactory){
            this.sqlSessionFactory = sqlSessionFactory;
        }


        public List<CommentsDTO> getAll(){
            SqlSession session = sqlSessionFactory.openSession();
            CommentsMapper mapper = session.getMapper(CommentsMapper.class);
            List<CommentsDTO> list = mapper.getAll();
            return list;
        }

    public List<CommentsDTO> selectByFoodName(String foodName){
        SqlSession session = sqlSessionFactory.openSession();
        CommentsMapper mapper = session.getMapper(CommentsMapper.class);
        List<CommentsDTO> list = mapper.selectByFoodName(foodName);
        return list;
    }

        private boolean enrollComments(String memberId) {
            List<MemberDTO> list = null;
            SqlSession session = sqlSessionFactory.openSession();
            MemberMapper mapper = session.getMapper(MemberMapper.class);
            try {
                list = mapper.selectById(memberId);
                session.commit();
            }catch (Exception e){
                e.printStackTrace();
                session.rollback();
            } finally {
                session.close();
            }
            return !list.isEmpty();
        }

}
