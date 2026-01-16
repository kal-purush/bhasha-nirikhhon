package com.onepagecrm.samples;

import com.onepagecrm.OnePageCRM;
import com.onepagecrm.exceptions.OnePageException;
import com.onepagecrm.models.Company;
import com.onepagecrm.models.CompanyList;
import com.onepagecrm.models.CustomField;
import com.onepagecrm.models.User;
import com.onepagecrm.net.request.Request;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class AND328DebugFieldsDriver {

    private static final Logger LOG = Logger.getLogger(AND328DebugFieldsDriver.class.getName());

    public static void main(String[] args) throws OnePageException {
        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("config.properties");

            // Load the properties file
            prop.load(input);

        } catch (IOException e) {
            LOG.severe("Error loading the config.properties file");
            LOG.severe(e.toString());
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    LOG.severe("Error closing the config.properties file");
                    LOG.severe(e.toString());
                }
            }
        }

        OnePageCRM.setServer(Request.DEV_SERVER);

        User loggedInUser = User.login(
                prop.getProperty("username"),
                prop.getProperty("password"));

        CompanyList companies = loggedInUser.companies();
        Company company = Company.getSingleCompany(companies.get(0).getId());

        List<CustomField> customFieldList = CustomField.listContacts();
        List<CustomField> companyFieldList = CustomField.listCompanies();

        LOG.info("Companies : " + companies);
        LOG.info("Company : " + company);
        LOG.info("Custom fields : " + customFieldList);
        LOG.info("Company fields : " + companyFieldList);

        for (CustomField field : customFieldList) {
            LOG.info("CustomField[index: " + customFieldList.indexOf(field)
                    + ", hashCode: " + field.getId().hashCode() + "] : " + field);
        }

        for (CustomField field : companyFieldList) {
            LOG.info("CompanyField[index: " + companyFieldList.indexOf(field)
                    + ", hashCode: " + field.getId().hashCode() + "] : " + field);
        }
    }
}