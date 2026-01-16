package modules.controller;


import io.swagger.annotations.ApiOperation;
import modules.dto.MainCommentDto;
import modules.entity.MainComment;
import modules.service.IMainCommentService;
import modules.vo.Result;
import org.springframework.beans.BeanUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.validation.Valid;

/**
 * <p>
 * 第一级评论表 前端控制器
 * </p>
 *
 * @author kangshizhu
 * @since 2022-09-20
 */
@RestController
@RequestMapping("/mainComment")
public class MainCommentController {


    @Resource
    IMainCommentService iMainCommentService;

    @ApiOperation(value = "一级评论添加接口", notes = "一级评论添加接口")
    @PostMapping(value = "/add")
    @ResponseBody
    public Result add(@RequestBody @Valid MainCommentDto mainCommentDto) {
        MainComment mainComment=new MainComment();
        BeanUtils.copyProperties(mainCommentDto,mainComment);
        mainComment.setThumbs(0);
        iMainCommentService.save(mainComment);
        return Result.OK("添加成功");
    }

}