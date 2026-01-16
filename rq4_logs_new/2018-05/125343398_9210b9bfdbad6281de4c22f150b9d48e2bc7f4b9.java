package kubernetes.client.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import kubernetes.client.model.ProactiveAutoscaler;

public interface ProactiveAutoscalerMapper {
	@Insert("INSERT INTO proactive_autoscaler(pa_name, min_replicas, max_replicas, max_cpu, app_id) "
			+ "VALUES(#{pa.name}, #{pa.minReplicas}, #{pa.maxReplicas}, #{pa.maxCPU}, #{appId})")
	void insert(@Param("pa") ProactiveAutoscaler pa, @Param("appId") int appId);

	@Select("SELECT * FROM proactive_autoscaler WHERE app_id = #{appId}")
	@Results({ @Result(property = "name", column = "pa_name"),
			@Result(property = "minReplicas", column = "min_replicas"),
			@Result(property = "maxReplicas", column = "max_replicas"),
			@Result(property = "maxCPU", column = "max_cpu") })
	ProactiveAutoscaler getPAByAppId(int appId);

	@Update("UPDATE proactive_autoscaler SET min_replicas=#{pa.minReplicas}, max_replicas=#{pa.maxReplicas}, max_cpu=#{pa.maxCPU} WHERE app_id =#{appId}")
	void update(@Param("pa") ProactiveAutoscaler pa, @Param("appId") int appId);

	@Delete("DELETE FROM proactive_autoscaler WHERE app_id =#{appId}")
	void delete(int appId);
}