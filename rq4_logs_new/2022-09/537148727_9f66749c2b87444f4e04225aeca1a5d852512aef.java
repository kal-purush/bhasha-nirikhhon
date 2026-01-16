package modules.vo;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import modules.entity.BaseEntity;
import modules.entity.MainComment;

import java.util.List;

/**
 * <p>
 * 
 * </p>
 *
 * @author astupidcoder
 * @since 2022-09-17
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
@ApiModel(value="Exhibition对象", description="")
public class ExhibitionReturnVo extends BaseEntity {

    private static final long serialVersionUID = 1L;

    @ApiModelProperty(value = "主键id")
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    @ApiModelProperty(value = "导航栏名称")
    private String labelName;

    @ApiModelProperty(value = "导航栏navigation关联的id",required = true)
    private Long navigationId;

    @ApiModelProperty(value = "发布作品人id 关联user的id")
    private Long usersId;

    @ApiModelProperty(value = "点赞数量")
    private Integer thumbs;

    @ApiModelProperty(value = "作品路径list")
    private String exhibitionUrl;

    @ApiModelProperty(value = "关注数量")
    private Integer follows;

    @ApiModelProperty(value = "介绍")
    private String introduce;

    @ApiModelProperty(value = "一级评论vo")
    private List<MainCommentReturnVo> mainCommentReturnVoList;

}