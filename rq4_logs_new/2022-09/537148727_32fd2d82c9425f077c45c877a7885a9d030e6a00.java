package modules.dto;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

/**
 * @description:
 * @author:kangshizhu
 * @create:2022/9/23-22:39
 **/
@Data
public class ExhibitionIdDto {

    @NotNull(message = "用户id不能为空")
    @ApiModelProperty(value = "usersId",required = true)
    private Long usersId;

    @NotNull(message = "评论不能为空")
    @ApiModelProperty(value = "作品id",required = true)
    private Long exhibitionId;
}