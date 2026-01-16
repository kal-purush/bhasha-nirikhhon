package project.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import project.mockservice.MockDataService;
import project.responseentities.DriverRiderResponse;
import project.service.UserService;

/**
 * Created by olafurorn on 10/1/15.
 */
@Controller
public class DriverRiderController
{
    private static final String LOGTAG = DriverRiderController.class.getSimpleName();

    private final UserService userModel;
    private final MockDataService mockDataService;


    @Autowired
    public DriverRiderController(UserService userModel, MockDataService mockDataService)
    {
        this.userModel = userModel;
        this.mockDataService = mockDataService;
    }

    @RequestMapping(value = "/driverrider", method = RequestMethod.GET)
    public ResponseEntity<DriverRiderResponse> login(
            //@RequestParam(value="accessToken") String accessToken, TODO: check the accessToken
    )
    {
        // user exist and we return 200
        DriverRiderResponse driverRiderResponse = new DriverRiderResponse(
                mockDataService.getMockDriverList(),
                mockDataService.getMockRiderList());
        return new ResponseEntity<DriverRiderResponse>(driverRiderResponse, HttpStatus.OK);
    }

}