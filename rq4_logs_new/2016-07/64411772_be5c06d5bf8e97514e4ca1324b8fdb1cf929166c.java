package com.service.app.appService;

import com.service.app.dao.*;
import com.service.app.models.BloodBank;
import com.service.app.models.BloodBankRequest;
import com.service.app.models.BloodDonors;
import com.service.app.util.Messages;
import hms.kite.samples.api.SdpException;
import hms.kite.samples.api.StatusCodes;
import hms.kite.samples.api.ussd.MoUssdListener;
import hms.kite.samples.api.ussd.UssdRequestSender;
import hms.kite.samples.api.ussd.messages.MoUssdReq;
import hms.kite.samples.api.ussd.messages.MtUssdReq;
import hms.kite.samples.api.ussd.messages.MtUssdResp;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MainMenu implements MoUssdListener {

    private final static Logger LOGGER = Logger.getLogger(MainMenu.class.getName());

    private static final String EXIT_SERVICE_CODE = "000";
    private static final String BACK_SERVICE_CODE = "999";
    private static final String USSD_OP_MO_INIT = "mo-init";
    private static final String USSD_OP_MT_CONT = "mt-cont";
    private static final String USSD_OP_MT_FIN = "mt-fin";
    private static final String NEXT_SERVICE_CODE = "992";
    private static final String MAIN_SERVICE_CODE = "994";
    public static int level = 0;
    int count_level0 = 1;
    int count_level1 = 1;
    String init_key = "0";
    int pinCode;
    String donorName = "";
    String bloodGroup = "";
    String bloodType = "";
    String neededTime = "";
    String location = "";
    String display = "";
    String bloodBankName = "";
    String bloodBankLocation = "";
    private List<String> menuState = new ArrayList<String>();
    // service to send the request
    private UssdRequestSender ussdMtSender;

    ItemDao itemDao;

    @Override
    public void init() {
        // create and initialize service
        try {
            ussdMtSender = new UssdRequestSender(new URL(Messages.getMessage("sdp.server.url")));

        } catch (MalformedURLException e) {
            LOGGER.log(Level.INFO, "Unexpected error occurred", e);
        }
    }

    @Override
    public void onReceivedUssd(final MoUssdReq moUssdReq) {
        try {
            // start processing request
            processRequest(moUssdReq);
        } catch (SdpException e) {
            LOGGER.log(Level.INFO, "Unexpected error occurred", e);
        }
    }


    private void processRequest(final MoUssdReq moUssdReq) throws SdpException {
        // exit request - session destroy
        if (moUssdReq.getMessage().equals(EXIT_SERVICE_CODE)) {
            terminateSession(moUssdReq);
            return;// completed work and return
        }

        // back button handling
        if (moUssdReq.getMessage().equals(BACK_SERVICE_CODE)) {
            backButtonHandle(moUssdReq);
            return;// completed work and return
        }

        // get current service code
        String serviceCode;
        if (USSD_OP_MO_INIT.equals(moUssdReq.getUssdOperation())) {
            serviceCode = "0";
            level = 0;
            clearMenuState();

        } else {
            if (moUssdReq.getMessage().equals(NEXT_SERVICE_CODE)) {
                if (level == 0) {
                    count_level0 += 1;
                } else if (level == 1) {
                    count_level1 += 1;
                }
                serviceCode = getServiceCode(moUssdReq);
            } else {
                serviceCode = getServiceCode(moUssdReq);
                level = level + 1;
            }
        }
        // create request to display user
        final MtUssdReq request = createRequest(moUssdReq, buildMenuContent(serviceCode), USSD_OP_MT_CONT);
        sendRequest(request);
        // record menu state
        menuState.add(serviceCode);
    }


    private MtUssdReq createRequest(final MoUssdReq moUssdReq, final String menuContent, final String ussdOperation) {
        final MtUssdReq request = new MtUssdReq();
        request.setApplicationId(moUssdReq.getApplicationId());
        request.setEncoding(moUssdReq.getEncoding());
        request.setMessage(menuContent);
        request.setPassword(Messages.getMessage(moUssdReq.getApplicationId()));
        request.setSessionId(moUssdReq.getSessionId());
        request.setUssdOperation(Messages.getMessage("operation"));
        request.setVersion(moUssdReq.getVersion());
        request.setDestinationAddress(moUssdReq.getSourceAddress());
        return request;
    }

    private String getText(final String key) {
        String outputString = "";

        if (level == 0) {
            outputString = "Welcome to SERVICE PROVIDER \n1.Shop owners\n2.Service stations\n3.Service request\n000.Exit";
            System.out.println("level 0 :" + level);
        }
        if (level > 0) {
            if (level == 1) {
                init_key = key;
                if ("1".equals(init_key)) {
                    outputString = getBlood_Bank(key);
                } else if ("2".equals(init_key)) {
                    outputString = getBlood_Donar(key);
                } else outputString = outputString + "Invalid Selection" + "\n999:Back" + key;
            } else {

                if ("1".equals(init_key)) {
                    outputString = getBlood_Bank(key);
                } else if ("2".equals(init_key)) {
                    outputString = getBlood_Donar(key);
                } else outputString = outputString + "Invalid Selection" + "\n999:Back" + key;
            }
        }
        return outputString;
    }

    private String getBlood_Bank(final String key) {
        String outputString = "";
        if (level == 1) {
            outputString = outputString + "Please enter your pin code:";

        } else if (level == 2) {
            pinCode = Integer.parseInt(key);

            BloodBankDao bbd = new BloodBankDao();
            BloodBank bb = new BloodBank();
            //List validBloodBank=bb.getBloodBank(pinCode);
            List t = bbd.validPinCode(pinCode);

            if (!t.isEmpty()) {
                outputString = outputString + "Select an item you want service: \n 1. Car \n 2. Mobile \n 2. Laptop \n 2. TV \n999:Back";
            } else {
                outputString = outputString + "Invalid pin code! Try again\n999:Back\n000:Exit";
            }
        } else if (level == 3) {
            bloodGroup = getbloodgroup(key);
            outputString = outputString + "Select your Blood Type \n 1. Positive \n 2. Negative \n999:Back\n000:Exit";
        } else if (level == 4) {

            bloodType = getbloodtype(key);
            bloodGroup = bloodGroup + bloodType;

            outputString = "Select needed time:\n 1. Urgent \n 2.Ordinary \n999:Back\n000.Exit";
        } else if (level == 5) {
            neededTime = getneededtime(key);
            int my_id = 12466;
            BloodBankRequest bdr = new BloodBankRequest(my_id, pinCode, bloodGroup, neededTime);
            BloodBankRequestDao bd = new BloodBankRequestDao();
            bd.insert_donor_request_detail(bdr);
            outputString = outputString + "Your request has been successfully sent. We will inform donors as soon as possible. \n Thank you \n000.Exit";

        }


        return outputString;

    }

    private String getBlood_Donar(final String key) {
        //String outputString="you in Donor";        
        // return outputString;
        //int phn=0773400432;
        String outputString = "";
        if (level == 1) {
            outputString = outputString + "Enter your Name:";

        } else if (level == 2) {
            donorName = key;
            outputString = outputString + "Select your area \n1.Anaikoddai\n2.Chankanai\n3.Jaffna\n4.Nallur\n5.Thirunalvelly\n000.Exit";
        } else if (level == 3) {
            location = getlocation(key);
            outputString = outputString + "Select your blood group \n1.O\n2.A\n3.B\n4.AB\n000.Exit";
        } else if (level == 4) {
            bloodGroup = getbloodgroup(key);
            System.out.println(bloodGroup);
            outputString = outputString + "Select your blood group \n1.Positive\n2.Negative\n000.Exit";
        } else if (level == 5) {
            bloodGroup = bloodGroup + getbloodtype(key);
            // insert to table
            BloodDonors bdm = new BloodDonors(07730433, donorName, bloodGroup, location);
            BloodDonorsDao bdd = new BloodDonorsDao();
            bdd.insert_donor_detail(bdm);
            outputString = outputString + "You are now registered on RED-DROP we will contact you soon.\nThank you\n000.Exit";
        }

        return outputString;
    }

    public String getneededtime(String key) {
        if (Integer.parseInt(key) == 1) {
            return "Urgent";
        }
        if (Integer.parseInt(key) == 2) {
            return "Ordinary";
        } else return "Invalid selection";
    }

 /*   public String getbloodgroup(String key) {
        if (Integer.parseInt(key) == 1) {
            return "O";
        }
        if (Integer.parseInt(key) == 2) {
            return "A";
        }
        if (Integer.parseInt(key) == 3) {
            return "B";
        }
        if (Integer.parseInt(key) == 4) {
            return "AB";
        } else return "Invalid selection";
    }*/

    public String getItems(String key){
        List<String> items = itemDao.getItemsById(key);
        for(int i=0;i<items.size();i++){
            if(Integer.parseInt(key) == i){
                if(items.get(i).equals(itemDao.getDateByIdAndItem(key,)))
            }
        }
    }

    public String getbloodtype(String key) {
        if (Integer.parseInt(key) == 1) {
            return "+";
        }
        if (Integer.parseInt(key) == 2) {
            return "-";
        } else return "Invalid selection";
    }

    public String getlocation(String key) {
        if (Integer.parseInt(key) == 1) {
            return "Anaikoddai";
        }
        if (Integer.parseInt(key) == 2) {
            return "Chankanai";
        }
        if (Integer.parseInt(key) == 3) {
            return "Jaffna";
        }
        if (Integer.parseInt(key) == 4) {
            return "Nallur";
        }
        if (Integer.parseInt(key) == 5) {
            return "Thirunelvelly";
        } else return "Invalid selection";
    }


    private MtUssdResp sendRequest(final MtUssdReq request) throws SdpException {
        // sending request to service
        MtUssdResp response = null;
        try {
            response = ussdMtSender.sendUssdRequest(request);
        } catch (SdpException e) {
            LOGGER.log(Level.INFO, "Unable to send request", e);
            throw e;
        }

        // response status
        String statusCode = response.getStatusCode();
        String statusDetails = response.getStatusDetail();
        if (StatusCodes.SuccessK.equals(statusCode)) {
            LOGGER.info("MT USSD message successfully sent");
        } else {
            LOGGER.info("MT USSD message sending failed with status code [" + statusCode + "] " + statusDetails);
        }
        return response;
    }

    /**
     * Clear history list
     */
    private void clearMenuState() {
        LOGGER.info("clear history List");
        menuState.clear();
        //level=0;
    }

    /**
     * Terminate session
     *
     * @param moUssdReq
     * @throws SdpException
     */
    private void terminateSession(final MoUssdReq moUssdReq) throws SdpException {
        final MtUssdReq request = createRequest(moUssdReq, "", USSD_OP_MT_FIN);
        sendRequest(request);
    }


    private void backButtonHandle(final MoUssdReq moUssdReq) throws SdpException {
        String lastMenuVisited = "0";

        // remove last menu when back
        if (menuState.size() > 0) {
            menuState.remove(menuState.size() - 1);
            lastMenuVisited = menuState.get(menuState.size() - 1);
            level = level - 1;
        }

        // create request and send
        final MtUssdReq request = createRequest(moUssdReq, buildMenuContent(lastMenuVisited), USSD_OP_MT_CONT);

        sendRequest(request);

        // clear menu status
        if ("0".equals(lastMenuVisited)) {
            clearMenuState();
            // add 0 to menu state ,finally its in main menu
            menuState.add((String) "0");
        }

    }


    private String getServiceCode(final MoUssdReq moUssdReq) {
        //byte serviceCode = 0;
        String serviceCode = "0";
        try {
            //serviceCode = Byte.parseByte(moUssdReq.getMessage());
            serviceCode = moUssdReq.getMessage();
        } catch (NumberFormatException e) {
            return serviceCode;
        }
        // create service codes for child menus based on the main menu codes
        return serviceCode;
    }

    private String buildMenuContent(final String selection) {
        String menuContent;
        try {
            // build menu contents           
            menuContent = getText(selection);
        } catch (MissingResourceException e) {
            // back to main menu
            menuContent = getText((String) "0");
        }
        return menuContent;
    }


}