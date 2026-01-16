/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package converters;
import com.google.inject.Inject;
import facade.GroupFacadeLocal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import models.Group;
import org.apache.struts2.util.StrutsTypeConverter;
/**
 *
 * @author sergio
 */
public class GroupConverter extends StrutsTypeConverter {
    
    @Inject
    private GroupFacadeLocal groupFacadeLocal;

    @Override
    public Object convertFromString(Map map, String[] values, Class type) {
        Set<Group> groups = new HashSet();
        for(String value: values){
            Group group = groupFacadeLocal.find(value);
            groups.add(group);
        }
        return groups;
    }

    @Override
    public String convertToString(Map map, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}