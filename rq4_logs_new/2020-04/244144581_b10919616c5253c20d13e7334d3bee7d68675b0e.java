package gp.lcw.sd.by.g3p.extension.rest.notice;

import gp.lcw.sd.by.g3p.base.rest.GenericController;
import gp.lcw.sd.by.g3p.extension.dao.notice;
import gp.lcw.sd.by.g3p.extension.domain.noticeDaoOperate;
import gp.lcw.sd.by.g3p.extension.serviceManager.noticeManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@RequestMapping("/notice")
@RestController
public class noticeOperate  extends GenericController<notice,Long, noticeManager> {




    @Autowired
    noticeDaoOperate noticeDaoOperate;

    @RequestMapping(value = "Add.json" ,method = RequestMethod.POST)
    @ResponseBody
    public String Add(@RequestBody notice notice){
        String returnFlag;

        System.out.println("输出通告"+notice.getNoticeTitle());
        noticeDaoOperate.save(notice);
        returnFlag="添加成功";
        return returnFlag;
    }

    @RequestMapping(value = "Delete.json" ,method = RequestMethod.POST)
    @ResponseBody
    public String Delete(@RequestParam(name = "id",required = true) Long id){
        String returnFlag;
        noticeDaoOperate.deleteById(id);
        returnFlag="删除成功";
        return returnFlag;
    }

    @RequestMapping(value = "makeTop.json" ,method = RequestMethod.POST)
    @ResponseBody
    public String makeTop(@RequestParam(name = "id",required = true) Long id){
        String returnFlag;

        List<notice>noticeList=noticeDaoOperate.findAll();
        for(int i=0;i<noticeList.size();i++)
        {
            notice notices=noticeList.get(i);
            notices.setTopFlag("false");
            noticeDaoOperate.save(notices);
        }

        notice notice=noticeDaoOperate.findById(id).get();
        notice.setTopFlag("true");
        noticeDaoOperate.save(notice);

        returnFlag="置顶成功";
        return returnFlag;
    }
    @RequestMapping(value = "findAll.json" ,method = RequestMethod.GET)
    @ResponseBody
    public List<notice> findAll(){

        List<notice>noticeList=noticeDaoOperate.findAll();

        return noticeList;
    }

}