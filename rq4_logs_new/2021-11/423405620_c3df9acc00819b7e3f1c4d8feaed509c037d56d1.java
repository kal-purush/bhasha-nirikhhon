package ruangong.our_land.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Author hwy
 * @Date 2021/11/25
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserDto {

    /** 用户id */
    private Integer id;

    /** 用户名 */
    private String user_name;

    /** 用户密码 */
    private String password;

    /** 游戏进度 */
    private Integer progress;

    /** 经验值 */
    private Integer exp;

    /** 精灵背包 */
    private List<Integer> spirits_bag;

    /** 道具背包 */
    private List<Integer> props_bag;

}