import { ErrorStateMatcher } from "@angular/material";
import { FormControl, FormGroupDirective, NgForm } from "@angular/forms";

export class FormChecker implements ErrorStateMatcher {
    
      isErrorState(control: FormControl | null, form: FormGroupDirective | NgForm | null): boolean {
        const isSubmitted = form && form.submitted;
        return this.errorStatus;
      }
      private _errorStatus:boolean;
      
      constructor(
        private _errorMsg:string
      ){}
    
      get errorStatus():boolean{
        return this._errorStatus;
      }
    
      set errorStatus(value:boolean){
        this._errorStatus = value;
      }
    
      set errorMsg(value:string){
        this._errorMsg=value;
      }
      get errorMsg():string{
        return this._errorMsg;
      }
    
    }