package com.qihang.controller;

import com.qihang.dao.ProjectDao;
import com.qihang.services.*;
import com.qihang.servicesImp.PrgrmSklServiceImp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

@Controller
@RequestMapping("/employer")
public class EmployerController {
    @Autowired
    private EmployerService employerService;
    @Autowired
    private ProgrammerProjectService programmerProjectService;
    @Autowired
    private ProjectService projectService;
    @Autowired
    private EmpProjectService empProjectService;
    @Autowired
    private PrgrmSklServiceImp prgrmSklServiceImp;
    @Autowired
    private ProgrammerService programmerService;
    @Autowired
    private ProgrammerSkillService programmerSkillService;

    @RequestMapping(value = "/sigout")
    public String sigout(HttpServletRequest req){
        req.getSession().invalidate();
        return "index";
    }

    /**
     * 雇主注册
     * @param eId
     * @param password
     * @param name
     * @param phone
     * @param email
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/sigin",method = RequestMethod.GET)
    public boolean sigin(@RequestParam String eId,@RequestParam String password,@RequestParam String name,@RequestParam String phone,@RequestParam String email ){
        return employerService.ifSigin(eId,password,name,phone,email);
    }

    /**
     * 雇主登陆
     * @param eId
     * @param password
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/login",method = RequestMethod.GET)
    public boolean sigin(@RequestParam String eId,@RequestParam String password,HttpServletRequest req ){
        HttpSession session = req.getSession();
        boolean login = employerService.ifLogin(eId,password);
        if(login==true){
            session.setAttribute("empId", eId);
        }
        return login;
    }

    /**
     * 获取雇主信息
     * @param eId
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/info",method = RequestMethod.POST)
    public Map getInfo(@RequestParam String eId){
        Map map = new HashMap();
        map = employerService.getEmpInfo(eId);
        return map;
    }

    /**
     * 修改雇主信息
     * @param eId
     * @param name
     * @param phone
     * @param email
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/updateInfo",method = RequestMethod.POST)
    public boolean updateInfo(@RequestParam String eId,@RequestParam String name,@RequestParam String phone,@RequestParam String email){
        return employerService.updateEmpInfo(eId,name,phone,email);
    }

    /**
     * 密码修改
     * @param eId
     * @param password
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/passwordModify",method = RequestMethod.POST)
    public boolean passwordModify(@RequestParam String eId,@RequestParam String password){
        return employerService.updatePassword(eId,password);
    }

    /**
     * 获取雇主所有发布中项目
     * @param eId
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/getProject",method = RequestMethod.POST)
    public List<Map> getProejctStatus_1(@RequestParam String eId){
        return employerService.getProject(eId);
    }

    @ResponseBody
    @RequestMapping(value = "/getApplyProgrammer",method = RequestMethod.POST)
    public List<Map> getApplyProgrammer(@RequestParam String proId){
        if(proId==null||proId.equals("")){
            return null;
        }
        return employerService.getApplyProgrammer(proId);
    }

    /**
     * 签约人才
     * @param eId
     * @param pId
     * @param proId
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/chooseProgrammer",method = RequestMethod.POST)
    public boolean chooseProgrammer(@RequestParam String eId,@RequestParam String pId,@RequestParam String proId){
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String time = df1.format(new Date());
        Timestamp createDate = Timestamp.valueOf(time);
        int pro_id = Integer.parseInt(proId);
        return programmerProjectService.recepteApply(pro_id,  eId,  pId, createDate);
    }

    /**
     * 邀请人才
     * @param eId
     * @param proId
     * @param pId
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/insertNewInvite",method = RequestMethod.POST)
    public boolean insertNewInvite(@RequestParam String eId,@RequestParam String proId,@RequestParam String pId){
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String time = df1.format(new Date());
        Timestamp createDate = Timestamp.valueOf(time);
        int a =programmerProjectService.insertNewInvite(eId,proId,pId,createDate);
        if(a>=1){
            return true;
        }else{
            return false;
        }
    }

    /**
     * 发布项目
     * @param name
     * @param eId
     * @param proField
     * @param skill1
     * @param skill2
     * @param lastDate
     * @param proMoney
     * @param overview
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/addProject",method = RequestMethod.POST)
    public boolean addProject(@RequestParam String name,@RequestParam String eId,@RequestParam String proField,@RequestParam int skill1,
                              @RequestParam int skill2, @RequestParam Date lastDate,@RequestParam long proMoney,@RequestParam String overview){
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String time = df1.format(lastDate);
        Timestamp createDate = Timestamp.valueOf(time);
        return projectService.insertProject(name,eId,proField,skill1,skill2,proMoney,createDate,overview,"1");
    }

    @ResponseBody
    @RequestMapping(value = "/project1",method = RequestMethod.POST)
    public List<Map> getProject1(@RequestParam String eId){
        return empProjectService.getProject1(eId);
    }

    @ResponseBody
    @RequestMapping(value = "/project2",method = RequestMethod.POST)
    public List<Map> getProject2(@RequestParam String eId){
        return empProjectService.getProject2(eId);
    }

    @ResponseBody
    @RequestMapping(value = "/project3",method = RequestMethod.POST)
    public List<Map> getProject3(@RequestParam String eId){
        return empProjectService.getProject3(eId);
    }

    /**
     * 项目评价
     * @param eId
     * @param proId
     * @param pId
     * @param fieldScore
     * @param skillId1
     * @param skillId2
     * @param skillScore1
     * @param skillScore2
     * @param overScore
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/projectComplete",method = RequestMethod.POST)
    public boolean projectComplete(@RequestParam String eId,@RequestParam String proId,@RequestParam String pId,
                                     @RequestParam int fieldScore,@RequestParam int skillId1, @RequestParam int skillId2 ,
                                     @RequestParam int skillScore1,@RequestParam int skillScore2,@RequestParam double overScore){
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String time = df1.format(new Date());
        Timestamp createDate = Timestamp.valueOf(time);
        Float a = (Float) prgrmSklServiceImp.selectSkillById(pId).get(0).get("skill_score");
        float temp1 = (float)(a*0.8+skillScore1*0.2);
        Float b = (Float) prgrmSklServiceImp.selectSkillById(pId).get(1).get("skill_score");
        float temp2 = (float)(b*0.8+skillScore2*0.2);
        prgrmSklServiceImp.programmerSkillUpdate(pId,skillId1,temp1);
        prgrmSklServiceImp.programmerSkillUpdate(pId,skillId1,temp2);
        double c = overScore*0.3*20 + programmerService.selectById(pId).getOverScore()*0.7;
        programmerService.updateScore(c,pId);
        return programmerProjectService.ifComplete(Integer.parseInt(proId),eId,pId,createDate);
    }

    /**
     * 人才推荐
     * @param proId
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/suggestProgrammer",method = RequestMethod.POST)
    public List<Map> suggestProgrammer(@RequestParam int proId){
        ArrayList<Map> arrayList = programmerService.selectAllProgrammer();
        double[] matchDegreeArray = new double[arrayList.size()];
        System.out.println(arrayList.get(0).get("p_id"));
        System.out.println(programmerSkillService.selectSkillById((String)(arrayList.get(0).get("p_id"))));
        for (int i=0;i<arrayList.size();i++) {
            int matchDegree = 0;
            if(arrayList.get(i).get("work_field").equals(projectService.selectById(proId).get("pro_field"))){
                matchDegree+=5;
            }
            if(programmerSkillService.selectSkillById((String)(arrayList.get(i).get("p_id"))).get(0).get("skill_id").equals(projectService.selectById(proId).get("pro_skill1"))){
                matchDegree+=8*(Float)programmerSkillService.selectSkillById((String)(arrayList.get(i).get("p_id"))).get(0).get("skill_score")/100;
            }
            if(programmerSkillService.selectSkillById((String)(arrayList.get(i).get("p_id"))).get(0).get("skill_id").equals(projectService.selectById(proId).get("pro_skill2"))){
                matchDegree+=8*(Float)programmerSkillService.selectSkillById((String)(arrayList.get(i).get("p_id"))).get(0).get("skill_score")/100;
            }
            if(programmerSkillService.selectSkillById((String)(arrayList.get(i).get("p_id"))).get(1).get("skill_id").equals(projectService.selectById(proId).get("pro_skill1"))){
                matchDegree+=8*(Float)programmerSkillService.selectSkillById((String)(arrayList.get(i).get("p_id"))).get(1).get("skill_score")/100;
            }
            if(programmerSkillService.selectSkillById((String)(arrayList.get(i).get("p_id"))).get(1).get("skill_id").equals(projectService.selectById(proId).get("pro_skill2"))){
                matchDegree+=8*(Float)programmerSkillService.selectSkillById((String)(arrayList.get(i).get("p_id"))).get(1).get("skill_score")/100;
            }
            matchDegreeArray[i]=matchDegree*(Integer)arrayList.get(i).get("over_score")/100;
        }
        int max = 0;
        double maxDegree = matchDegreeArray[0];
        for(int i =1;i<arrayList.size();i++){
            if(matchDegreeArray[i]>maxDegree){
                max = i;
                maxDegree = matchDegreeArray[i];
            }
        }
        List<Map> newList = new ArrayList<Map>();
        newList.add(arrayList.get(max));
        return newList;
    }
}