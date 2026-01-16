package com.vcode.controller;

import com.vcode.Handler.TestCaseHandler;
import com.vcode.Impl.ProblemDaoImpl;
import com.vcode.common.ResponseCode;
import com.vcode.config.TestCaseConfig;
import com.vcode.entity.Problem;
import com.vcode.entity.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.Map;
import java.util.logging.Logger;

@RestController
@RequestMapping("/admin/problem")
public class ProblemAdminController {

  @Autowired
  private ProblemDaoImpl problemDao;

  @Autowired
  private TestCaseConfig testCaseConfig;

  private Logger log = Logger.getLogger("ProblemAdminController");

  /**
   * @param problem problem的字段
   * @return com.vcode.entity.Response
   * @Description 创建problem的信息
   * @Date 2020/2/11 15:36
   */
  @PostMapping("/create")
  public Response createProblem(@RequestBody @Valid Problem problem) {
    Response res = new Response();
    // 添加origin前缀
    problem.setOriginId(problem.getOriginId());
    System.out.println("admin");
    // check is this problem exist
    if (problemDao.isExist(problem)) {
      res.setCode(ResponseCode.FAIL);
      res.setMessage("ID or Title is repeat");
      return res;
    }
    // check testCaseId
    String fileFullPath = "/tmp/" + problem.getTestCaseId() + ".zip";
    String targetDir = problem.getTestCaseId();
    if (!TestCaseHandler.isZipExist(fileFullPath)) {
      res.setCode(ResponseCode.FAIL);
      res.setMessage("Test Case is need to upload");
      return res;
    }
    // process testCaseFile: mv the file to target dir
    TestCaseHandler.moveTestCaseFiles(testCaseConfig.getPath(), targetDir);
    problemDao.saveProblem(problem);
    log.info(String.format("create problem: origin_id: %s, title: %s", problem.getOrigin(), problem.getTitle()));
    res.setData(problem);
    return res;
  }

  /**
   * @param problem problem的字段
   * @return com.vcode.entity.Response
   * @Description 编辑problem的信息
   * @Date 2020/2/11 15:35
   */
  @PostMapping("/edit")
  public Response editProblem(@RequestBody @Valid Problem problem) {
    Response res = new Response();
    if (!problem.getOriginId().startsWith(problem.getOrigin()+"-"))
      problem.setOriginId(problem.getOriginId());
    String err = problemDao.updateProblem(problem);
    if(err == null) {
      return res;
    }
    res.setCode(ResponseCode.ERROR);
    res.setMessage(err);
    return res;
  }

  /**
   * @param originId originId
   * @return com.vcode.entity.Response
   * @Description 根据OriginId删除problem
   * @Date 2020/2/11 15:36
   */
  @DeleteMapping("/delete")
  public Response deleteProblemByOriginId(@RequestParam(value = "originId") String originId) {
    Response res = new Response();
    if (originId == null) {
      res.setCode(ResponseCode.FAIL);
      res.setMessage("originId is required");
      return res;
    }
    problemDao.deleteProblemByOriginId(originId);
    return res;
  }

  /**
   * @param map get originId and visible by map
   * @return com.vcode.entity.Response
   * @Description update problem's visible
   * @Date 2020/2/25 14:57
   */
  @PostMapping("/visible")
  public Response changeVisible(@RequestBody Map<String, String> map) {
    Response res = new Response();
    String originId = map.get("originId");
    String visibleStr = map.get("visible");
    if (originId == null || visibleStr == null) {
      res.setCode(ResponseCode.FAIL);
      res.setMessage("originId and visible are required");
      return res;
    }
    if (!problemDao.isExist(originId)) {
      res.setCode(ResponseCode.FAIL);
      res.setMessage("The problem is not exist");
      return res;
    }
    boolean visible = Boolean.parseBoolean(visibleStr);
    problemDao.updateProblemVisible(originId, visible);
    return res;
  }
}