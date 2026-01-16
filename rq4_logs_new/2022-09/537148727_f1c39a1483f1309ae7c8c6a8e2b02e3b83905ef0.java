//package modules.controller;
//
//
//import com.alibaba.fastjson.JSONObject;
//import io.swagger.annotations.Api;
//import io.swagger.annotations.ApiOperation;
//import modules.dto.UsersDto;
//import modules.dto.WeiXinLoginDTO;
//import modules.entity.Users;
//import modules.service.IFileService;
//import modules.service.IUsersService;
//import modules.vo.Result;
//import org.springframework.web.bind.annotation.*;
//
//import javax.annotation.Resource;
//import javax.validation.Valid;
//import java.io.File;
//import java.util.List;
//
///**
// * <p>
// *  文件上传
// * </p>
// *
// * @author kangshizhu
// * @since 2022-09-16
// */
//@Api(tags = "阿里云文件上传接口")
//@RestController
//@RequestMapping("/file")
//public class FileController {
//   @Resource
//   IFileService iFileService;
//
//
//    /**
//     * 阿里云文件上传
//     * @param file
//     * @return
//     */
//    @ApiOperation("阿里云文件上传")
//    @RequestMapping(value = "/upload", method = RequestMethod.POST)
//    public Result upload(File file){
//        return iFileService.upload(file);
//
//    }
//
//}