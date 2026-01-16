// Code goes here!
class Department {
  protected employees: string[] = [];

  constructor(private readonly id: string, public name: string) {}

  describe() {
    console.log("Department: " + this.id + " " + this.name);
  }

  addEmployee(employee: string) {
    this.employees.push(employee);
    if (name === "Max") {
      return;
    }

    this.employees.push(name);
  }

  printEmployeeInformation() {
    console.log(this.employees.length);
    console.log(this.employees);
  }
}

class ITDepartment extends Department {
  admins: string[];
  constructor(id: string, admins: string[]) {
    super(id, "IT");
    this.admins = admins;
  }
}

const it = new ITDepartment("d1", ["Max"]);
it.addEmployee("Max");
it.addEmployee("Manu");
it.addEmployee("Anne");

it.describe();
it.name = "NEW NAME";
it.printEmployeeInformation();
console.log(it);

class AccountingDepartment extends Department {
  private lastReport: string;

  get mostRecentReport() {
    if (this.lastReport) {
      return this.lastReport;
    }
    throw new Error("No report found");
  }

  set mostRecentReport(value: string) {
    if (!value) {
      throw new Error("Please pass in a valid value");
    }
    this.addReport(value);
  }

  constructor(id: string, private reports: string[]) {
    super(id, "IT");
    this.lastReport = reports[0];
  }

  addEmployee(name: string) {
    if (name === "max") {
      return;
    }
    this.employees.push(name);
  }

  addReport(text: string) {
    this.reports.push(text);
    this.lastReport = text;
  }

  printReports() {
    console.log(this.reports);
  }
}

const accounting = new AccountingDepartment("d2", []);

accounting.mostRecentReport = "";

accounting.addReport("Something went wrong ...");
accounting.addEmployee("Max");
accounting.addEmployee("test");

accounting.printReports();