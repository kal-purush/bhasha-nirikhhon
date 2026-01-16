package Business;

import Business.Employee.Employee;
import Business.Employee.SecurityAdminEmployee;
import Business.Employee.SecurityOfficierEmployee;
import Business.Employee.SecuritySupervisorEmployee;
import Business.Employee.SysadminEmployee;
import Business.Employee.TMSupervisorEmployee;
import Business.Employee.TMAdminEmployee;
import Business.Employee.TMAnalystEmployee;
import Business.Employee.TravellerEmployee;
import Business.Enterprise.Enterprise;
import Business.Enterprise.SecurityAuthorityEnterprise;
import Business.Enterprise.TrainAuthorityEnterprise;
import Business.Network.Network;
import Business.Organization.Organization;
import Business.Role.SecurityAdminRole;
import Business.Role.SecurityOfficierRole;
import Business.Role.SecuritySupervisorRole;
import Business.Role.SystemAdminRole;

import Business.Role.TMAdminRole;
import Business.Role.TMAnalystRole;
import Business.Role.TMSupervisorRole;
import Business.Role.TravellerRole;

/**
 *
 * @author rrheg
 */
public class ConfigureASystem {
    
    public static EcoSystem configure(){
        
        EcoSystem system = EcoSystem.getInstance();
        
        Employee employee = system.getEmployeeDirectory().createEmployee("MANASI",new SysadminEmployee());        
        system.getUserAccountDirectory().createUserAccount("sa", "sa", employee, new SystemAdminRole());
       
        //  Create a network
        Network n = new Network();
        n.setName("Boston");
        system.getNetworkList().add(n);
        
         //Create an enterprise
        TrainAuthorityEnterprise tae = (TrainAuthorityEnterprise) system.getNetworkList().get(0).getEnterpriseDirectory().createAndAddEnterprise("MBTA", Enterprise.EnterpriseType.TRAIN_AUTHORITY_ENTERPRISE);
        SecurityAuthorityEnterprise sae = (SecurityAuthorityEnterprise) system.getNetworkList().get(0).getEnterpriseDirectory().createAndAddEnterprise("TPOLICE", Enterprise.EnterpriseType.SECURITY_AUTHORITY_ENTERPRISE); 
        
        //Create Enterprise
        Employee admin1 = tae.getEmployeeDirectory().createEmployee("Madhu Admin", new TMAdminEmployee());        
        tae.getUserAccountDirectory().createUserAccount("tadmin1", "tadmin1", admin1, new TMAdminRole());
        Employee police1 = sae.getEmployeeDirectory().createEmployee("Avindar Admin", new SecurityAdminEmployee());
        sae.getUserAccountDirectory().createUserAccount("tpolice1", "tpolice1", police1, new SecurityAdminRole());
        
        //Initialize some organizations
        Organization analystOrg = tae.getOrganizationDirectory().createOrganization(Organization.OrganizationType.TM_ANALYST_ORGANIZATION);
        Organization supOrg = tae.getOrganizationDirectory().createOrganization(Organization.OrganizationType.TM_SUPERVISOR_ORGANIZATION);
        //Organization driverOrg = tae.getOrganizationDirectory().createOrganization(Organization.OrganizationType.TM_DRIVER_ORGANIZATION);
        Organization tOrg =tae.getOrganizationDirectory().createOrganization(Organization.OrganizationType.TRAVELLER_ORGANIZATION);
        Organization sSupOrg = sae.getOrganizationDirectory().createOrganization(Organization.OrganizationType.SECURITY_SUPERVISOR_ORGANIZATION);
        Organization sOffOrg = sae.getOrganizationDirectory().createOrganization(Organization.OrganizationType.SECURITY_OFFICIER_ORGANIZATION);
        
        //create employees 
        Employee analyst1 = analystOrg.getEmployeeDirectory().createEmployee("Analyst-1",new TMAnalystEmployee());
        Employee supervisor1 = supOrg.getEmployeeDirectory().createEmployee("Supervisor-1",new TMSupervisorEmployee());
        Employee traveller1 = tOrg.getEmployeeDirectory().createEmployee("Traveller 1", new TravellerEmployee());
        Employee security1 = sSupOrg.getEmployeeDirectory().createEmployee("Security-1", new SecuritySupervisorEmployee());
        Employee officier1 = sOffOrg.getEmployeeDirectory().createEmployee("Officier-1", new SecurityOfficierEmployee());
        
        //create user account
        analystOrg.getUserAccountDirectory().createUserAccount("manasi", "manasi", analyst1, new TMAnalystRole());
        supOrg.getUserAccountDirectory().createUserAccount("tsuper1", "tsuper1", supervisor1, new TMSupervisorRole());
        tOrg.getUserAccountDirectory().createUserAccount("kunal", "kunal", traveller1, new TravellerRole());
        sSupOrg.getUserAccountDirectory().createUserAccount("psuper1", "psuper1", security1, new SecuritySupervisorRole());
        sOffOrg.getUserAccountDirectory().createUserAccount("officier1", "officier1", officier1, new SecurityOfficierRole());
      
        //updateDrivers(tae,driverOrg);
        
        return system;
    }
    
// private static void updateDrivers(TrainAuthorityEnterprise tae, Organization org){
//     
//     int j = 1;
//     for (Train t : tae.getTrainCatalog().getTrainsList()){     
//        TDriverEmployee driver1 = (TDriverEmployee) org.getEmployeeDirectory().createEmployee("TDriver" + j, new TDriverEmployee());
//        org.getUserAccountDirectory().createUserAccount("tdriver" + j, "tdriver" + j, driver1, new TDriverRole());
//        driver1.setTrainRunning(t);
//        driver1.setIsAssigned(true);      
//        t.setIsDriverAssigned(true);
//        j++;
//     }
//     
// }
 
 
 
 
 
}