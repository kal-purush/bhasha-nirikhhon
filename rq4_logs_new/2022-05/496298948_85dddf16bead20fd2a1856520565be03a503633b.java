package persistence.DAO;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import persistence.DTO.FoodStepDTO;
import persistence.DTO.MemberDTO;
import persistence.mapper.FoodStepMapper;
import persistence.mapper.MemberMapper;
import persistence.mapper.RecipeMapper;

import java.util.List;

public class FoodStepDAO {
    private SqlSessionFactory sqlSessionFactory = null;

    public FoodStepDAO(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }



    public List<FoodStepDTO> selectFoodStep(String foodName){
        List<FoodStepDTO> list = null;
        SqlSession session = sqlSessionFactory.openSession();
        FoodStepMapper mapper = session.getMapper(FoodStepMapper.class);

        try{
            list = mapper.selectFoodStep(foodName);
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            session.close();
        }
        return list;
    }
}