// import { company } from '../company';
import { IBudget, ICompanyUnit } from '../departments';
import { company } from '../hw-06';
import { IWorker } from '../workers';

interface IAccounting extends ICompanyUnit {
  preWorkersBallance: IWorker[];

  addPreWorkerToBallance(worker: IWorker): void;
  removePreWorkerFromBallance(worker: IWorker): void;
  paySallary(): void;
}

export class Accounting implements IAccounting {
  domain: string = 'accounting';

  preWorkersBallance: IWorker[] = [];

  budget: IBudget = { debit: 300000, credit: 0 };

  addPreWorkerToBallance(worker: IWorker): void {
    if (
      !this.preWorkersBallance.includes(worker) &&
      this.budget.debit - worker.sallary > 0 
      // && worker.status === 'pre-worker'
    ) {
      this.preWorkersBallance.push(worker);
      this.budget.debit -= worker.sallary;
      this.budget.credit += worker.sallary;
      // TODO: enum
      worker.changeStatus('pre-worker');
    } else {
      console.error(`Error: chek if ${worker.fullName} is no exists or budget compatibility`);
    }
  }

  removePreWorkerFromBallance(worker: IWorker): void {
    if (this.preWorkersBallance.includes(worker)) {
      this.preWorkersBallance = this.preWorkersBallance.filter((name) => name !== worker);
      this.budget.debit += worker.sallary;
      this.budget.credit -= worker.sallary;
      // TODO: enum
      worker.changeStatus('on-bench');
    } else {
      console.error(`Worker ${worker} does not exist in the department`);
    }
  }

  paySallary(): void {
    for (const worker of this.preWorkersBallance) {
      this.budget.debit -= worker.sallary;
      this.budget.credit += worker.sallary;
      worker.paySallary(worker.sallary);
    }

    for (const department of company.getDepartments()) {
      for (const worker of department.getWorkersList()) {
        department.removeFromBudget(worker.sallary);
        worker.paySallary(worker.sallary);
      }
    }
  }
}