package hello.controller;

import hello.domain.App;
import hello.domain.Company;
import hello.DAO.CompanyDao;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
/**
 * Created by Victor on 30-Jan-17.
 */
@RestController
@RequestMapping("company")
public class CompanyController {
    @Autowired
    private CompanyDao companyDao;

    private final Logger log = LoggerFactory.getLogger(CompanyController.class);

    /**
     * POST
     * Create a new app
     * @param company
     * @return
     */
    @RequestMapping(method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Company> create(@RequestBody Company company) {

        try {

            // Validando
            if (companyDao.findByName(company.getName())!=null) {

                log.error("Company name taken: " + company.getName());

                return new ResponseEntity(HttpStatus.BAD_REQUEST);

            }else{

                // Salvando
                companyDao.save(company);

            }
        }
        catch (Exception ex) {

            log.error("Error creating app "+ ex.getMessage());

            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);

        }
        return new ResponseEntity<Company>(company,HttpStatus.OK);
    }

    /**
     * GET
     * @param companyId
     * @return
     */
    @RequestMapping(value = {"/{companyId}"},
            method = RequestMethod.GET,
            produces="application/json")
    public ResponseEntity<Company>  get(@PathVariable(value = "companyId")  int companyId)
    {
        Company company = null;
        try {
            company = companyDao.findOne(companyId);
        }
        catch (Exception ex) {
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<Company>(company,HttpStatus.OK);
    }

    /**
     * DELETE
     * @param companyId
     * @return
     */
    @RequestMapping(value ="/{companyId}", method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity delete(@PathVariable(value = "companyId")  int companyId) {
        try {
            Company company = companyDao.findOne(companyId);
            companyDao.delete(company);
        }
        catch (Exception ex) {

            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        return new ResponseEntity<Company>(HttpStatus.OK);
    }

    /**
     * GET
     *
     * @param name
     * @return
     */
    @RequestMapping(value = "/get-by-name/{name}", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<Company> getByDomain(@PathVariable(value = "name") String name) {


        Company company = null;
        try {
            company = companyDao.findByName(name);
        }
        catch (Exception ex) {

            log.error("Error getByDomain "+ex.getMessage());
            return new ResponseEntity(HttpStatus.NOT_FOUND);

        }

        return new ResponseEntity<Company>(company,HttpStatus.OK);
    }

    /**
     * PUT
     * @param company
     * @return
     */
    @RequestMapping(
            method = RequestMethod.PUT,
            produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<Company> updateApp(@RequestBody Company company) {

        try {
            // Hace save, update o merge depndiendo el caso, lo detecta automaticamente
            company = companyDao.save(company);

            companyDao.save(company);
        }
        catch (Exception ex) {
            log.error( "Error updating the app: " + ex.toString());
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);

        }

        return new ResponseEntity<Company> (company,HttpStatus.OK);

    }


}