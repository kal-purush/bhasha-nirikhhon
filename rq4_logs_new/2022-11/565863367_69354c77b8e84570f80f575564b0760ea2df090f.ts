import { employees } from './model/Employees';
import { Employee, WorkingPossition } from './model/Model';


export default class DataService {

    private account: String = process.env.ACCOUNT;

    public getManagers(): Employee[] {
        return this.sortByPossition(WorkingPossition.MANAGER);
    }

    public getEngineers(): Employee[] {
        return this.sortByPossition(WorkingPossition.ENGINNER);
    }

    public getExperts(): Employee[] {
        return this.sortByPossition(WorkingPossition.EXPERT);
    }

    public getJuniors(): Employee[] {
        return this.sortByPossition(WorkingPossition.JUNIOR);
    }

    public getProgrammers(): Employee[] {
        return this.sortByPossition(WorkingPossition.PROGRAMMER);
    }

    public getAllEmployyes(): Employee[] {
        return employees;
    }

    public withAdmin(employyes: Employee[], admin: boolean) {
        const empls: Employee[] = [];
        for (const employee of employyes) {
            if (employee.admin === admin) {
                empls.push(employee);
            }
        }
        return empls;
    }


    private sortByPossition(possition: WorkingPossition): Employee[] {
        const empls: Employee[] = [];
        for (const employee of employees) {
            if (employee.possition === possition) {
                empls.push(employee);
            }
        }
        return empls;
    }

}