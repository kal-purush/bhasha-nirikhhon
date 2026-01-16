package org.vlir.darkaiv.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.logging.Level;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.vlir.darkaiv.model.Document;

//import org.vlir.darkaiv.model.Employee;
import org.vlir.darkaiv.services.DataServices;
import org.vlir.darkaiv.services.ExtractionServicesImpl;

@RestController
@RequestMapping("/document")
public class ExtractorController {

    @Autowired
    DataServices dataServices;

    @Autowired
//    ExtractionServices extractionServices;
    ExtractionServicesImpl extractionServices;

    static final Logger logger = Logger.getLogger(ExtractorController.class);

    @RequestMapping(value = "/workflow/test", method = RequestMethod.GET)
    public @ResponseBody
    String workflowTest() {

        File folder = new File("/home/daniel/Belgium_Work/Develop/github/DarkaivRestApi/Papers/");
        File[] listOfFiles = folder.listFiles();

        int count = 0;
        ArrayList<String> jsons = new ArrayList<>();
        for (File file : listOfFiles) {
            if (file.isFile()) {
                System.out.println("File " + file.getName());
                Document doc = new Document();
//        Document doc = null;
                try {
                    String json = extractionServices.processWorkflow(file);
                    jsons.add(json);


                } catch (Exception ex) {
                    java.util.logging.Logger.getLogger(RestController.class.getName()).log(Level.SEVERE, null, ex);

                }
            }
        }

        String jsn = "";
        for (String json : jsons) {
            jsn += json;
        }

        return jsn;

    }

    @RequestMapping(value = "/workflow", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody
    String workflow(@RequestBody File file) {
        String json = "";
//        Document doc = null;
        try {
            json = extractionServices.processWorkflow(file);
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(RestController.class.getName()).log(Level.SEVERE, null, ex);

        }

        return json;
    }

}