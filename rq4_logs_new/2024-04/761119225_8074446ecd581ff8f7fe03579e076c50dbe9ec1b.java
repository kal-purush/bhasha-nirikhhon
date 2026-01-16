import com.employee.info.mgmt.appl.facade.employee.EmployeeFacade;
import com.employee.info.mgmt.appl.facade.employee.impl.EmployeeFacadeImpl;
import com.employee.info.mgmt.data.employee.dao.EmployeeDao;
import com.employee.info.mgmt.data.employee.dao.impl.EmployeeDaoImpl;

public class EmployeeInfoMgtApplication {
    private EmployeeFacade employeeFacade;

    public EmployeeInfoMgtApplication() {
        EmployeeDao employeeDaoImpl = new EmployeeDaoImpl();
        this.employeeFacade = new EmployeeFacadeImpl(employeeDaoImpl);
    }

    public EmployeeFacade getEmployeeFacade() {
        return employeeFacade;
    }
}
