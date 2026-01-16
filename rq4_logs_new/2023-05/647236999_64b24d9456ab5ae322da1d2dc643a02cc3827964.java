import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AltGroupDS {

    public List<Object[]> fetchOrganizationGroupsData(Integer organisationID, Integer tenantID) throws Exception {
        StringBuilder sb = new StringBuilder();
        Query query = new Query();
        List<Object[]> resultList = null;
        try {
            sb.append("SELECT ag.AltGroupID, ag.OrganizationID, ag.TenantID, ag.EntityID, agp.SysGroupParameterID, " +
                    "sgp.SysGroupParameterName, agpv.ValueID, ag.ContextID, ag.IsActive FROM AltGroup ag " +
                    "join AltGroupParameter agp on agp.AltGroupID = ag.AltGroupID and agp.OrganizationID = :organizationID " +
                    "join SysGroupParameter sgp on sgp.SysGroupParameterID = agp.SysGroupParameterID " +
                    "join AltGroupParameterValue agpv on agpv.AltGroupParameterID = agp.AltGroupParameterID  and agpv.OrganizationID=:organizationID " +
                    "WHERE ag.OrganizationID=:organizationID and ag.TenantID=:tenantID and ag.IsActive=1 and agpv.IsActive=1;");
            query = getEntityManager("TalentPact").createNativeQuery(sb.toString());
            query.setParameter(":organizationID", organisationID);
            query.setParameter(":tenantID", tenantID);
            resultList = query.getResultList();
            return resultList;
        } catch (Exception e) {
            throw new Exception("Error while fetching group data for an organisation", e);
        }
    }

    public void saveValue(AltGroupParameterValueAggregate agpv) {
        getEntityManeger("TalentPact").save(agpv);
    }
}