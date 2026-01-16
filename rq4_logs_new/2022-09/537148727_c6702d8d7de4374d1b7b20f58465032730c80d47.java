package modules.controller;


import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.swagger.annotations.ApiOperation;
import modules.dto.ClassroomDto;
import modules.dto.UsersDto;
import modules.entity.Classroom;
import modules.entity.Users;
import modules.mapper.UsersMapper;
import modules.service.IUsersService;
import modules.util.RedisUtil;
import modules.vo.Result;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  用户表
 * </p>
 *
 * @author kangshizhu
 * @since 2022-09-16
 */
@RestController
@RequestMapping("/users")
public class UsersController {

    @Resource
    IUsersService iUsersService;



    @ApiOperation(value = "用户登录", notes = "用户登录")
    @PostMapping(value = "/queryUser")
    @ResponseBody
    public Result queryRuleUser(@RequestBody UsersDto usersDto) {
        Result<JSONObject> result=iUsersService.queryRuleUser(usersDto);
        return Result.OK(result);
    }
}