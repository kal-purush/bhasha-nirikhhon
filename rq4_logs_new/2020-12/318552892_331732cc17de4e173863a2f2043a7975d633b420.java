package com.zhang.libweb.web.book;


import com.zhang.libweb.constants.ResultDTO;
import com.zhang.libweb.enums.HttpCode;
import com.zhang.libweb.model.book.BookClassDTO;
import com.zhang.libweb.service.BookClassService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/bookClass")
public class BookClassController {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private BookClassService bookClassService;

    /**
     * 根据名称查找分类信息
     * @param name 分类名
     * @return
     */
    @RequestMapping("/findListByName")
    public ResultDTO findListByName(String name){
        try {
            return bookClassService.findListByName(name);
        }catch (Exception e){
            logger.error("系统异常："+e);
            return new ResultDTO(HttpCode.EXCEPTION.getCode(),"系统异常");
        }
    }

    /**
     * 根据Id查找数据
     * @param id
     * @return
     */
    @RequestMapping("/findById")
    public ResultDTO findById(int id){
        try {
            return bookClassService.selectByPrimaryKey(id);
        }catch (Exception e){
            logger.error("系统异常："+e);
            return new ResultDTO(HttpCode.EXCEPTION.getCode(),"系统异常");
        }
    }

    /**
     * 新增
     * @param bookClassDTO 新增实体
     * @return
     */
    @RequestMapping("/insert")
    public ResultDTO insert(BookClassDTO bookClassDTO){
        try {
            return bookClassService.insert(bookClassDTO);
        }catch (Exception e){
            logger.error("系统异常："+e);
            return new ResultDTO(HttpCode.EXCEPTION.getCode(),"系统异常");
        }
    }

    /**
     * 更新
     * @param bookClassDTO 更新实体
     * @return
     */
    @RequestMapping("/update")
    public ResultDTO updata(BookClassDTO bookClassDTO){
        try {
            return bookClassService.updateByPrimaryKey(bookClassDTO);
        }catch (Exception e){
            logger.error("系统异常："+e);
            return new ResultDTO(HttpCode.EXCEPTION.getCode(),"系统异常");
        }
    }

    /**
     * 根据Id删除数据
     * @param id 数据主键
     * @return
     */
    @RequestMapping("/delete")
    public ResultDTO delete(int id){
        try {
            return bookClassService.deleteByPrimaryKey(id);
        }catch (Exception e){
            logger.error("系统异常："+e);
            return new ResultDTO(HttpCode.EXCEPTION.getCode(),"系统异常");
        }
    }
}