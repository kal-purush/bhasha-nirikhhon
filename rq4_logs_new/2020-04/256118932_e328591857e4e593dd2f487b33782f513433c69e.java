package top.xxpblog.easyChat.api.vo.req;

import lombok.Data;
import top.xxpblog.easyChat.common.vo.req.BaseReqVO;

import javax.validation.constraints.NotEmpty;

@Data
public class UserRegisterReqVO extends BaseReqVO {

    @NotEmpty(message = "用户名不能为空")
    private String userName;
    /**
     * 密码
     */
    @NotEmpty(message = "密码不能为空")
    private String pwd;
}