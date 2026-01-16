package Application.controller.rest;

import Application.model.entities.AcademicDegree;
import Application.model.entities.Listener;
import Application.model.entities.TrainingProgram;
import Application.model.services.AbstractService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.catalina.mapper.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created by Rushan on 11.05.2016.
 */
@RestController
@RequestMapping("/data")
public class AcademicDegreeRest {

    @Resource(name = "academicDegreeServiceImpl")
    AbstractService service;

    @RequestMapping(value = "/academic_degree", method = RequestMethod.GET, produces = "application/json")
    public String getAll(HttpServletRequest request,
                         HttpServletResponse response) throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectMapper mapper = new ObjectMapper();
        List<AcademicDegree> degrees = service.getAll();
        mapper.writeValue(out, degrees);
        final byte[] data = out.toByteArray();
        return new String(data);
    }

    @RequestMapping(value = "/academic_degree/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String getSingle(@PathVariable int id,
                            HttpServletRequest request,
                            HttpServletResponse response) throws Exception {
        String result = "";
        AcademicDegree degree = (AcademicDegree) service.find(id);
        if (degree == null)
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        else
            result = new ObjectMapper().writeValueAsString(degree);
        return result;
    }

    @RequestMapping(value = "/academic_degree", method = RequestMethod.PUT)
    @ResponseBody
    public String putSingle(@RequestBody String body,
                            HttpServletRequest request,
                            HttpServletResponse response) throws Exception {
        JSONObject result = new JSONObject();
        if (body == null || "".equals(body))
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        else {
            AcademicDegree degree = new ObjectMapper().reader(AcademicDegree.class).readValue(body);
            result.put("entity_id", service.saveOrUpdate(degree));
        }
        return result.toString();
    }

    @RequestMapping(value = "/academic_degree/{id}", method = RequestMethod.DELETE)
    @ResponseBody
    public void deleteSingle(@RequestBody String body,
                             @PathVariable int id,
                             HttpServletRequest request,
                             HttpServletResponse response) throws Exception {
        AcademicDegree degree = (AcademicDegree) service.find(id);
        if (degree == null)
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        else
            service.remove(id);
    }


}