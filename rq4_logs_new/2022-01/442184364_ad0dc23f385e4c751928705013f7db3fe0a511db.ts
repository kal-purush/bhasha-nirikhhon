import { IEmployee } from '@/interfaces/IEmployee';
import { observable, action, computed, makeAutoObservable } from 'mobx';
class EmployeeStore {

    @observable
    private employees: IEmployee[];
    @observable
    private loading: boolean;

    constructor() {
        makeAutoObservable(this);
        this.employees = [];
        this.loading = false;
    };

    @computed
    get getEmployees(): IEmployee[] {
        return this.employees;
    };

    @computed
    get getLoading(): boolean {
        return this.loading;
    };

    public async fetch() {
        this.loading = true;
        const response = await fetch('http://localhost:4000/employees');
        const data = await response.json();
        this.loading = false;
        this.employees = data;
    }

};
export default new EmployeeStore();