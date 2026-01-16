import { FormControl } from "@angular/forms";

export class ExadelValidators {

    static restrictedFileTypes(control: FormControl): {[key: string]: boolean} {
       if(/.pdf$/.test(control.value) || /.doc$/.test(control.value) || /.docx$/.test(control.value)){
             return null
        } else {
            return {restrictedFileTypes: true}
        }
    }



    static restrictedGitHubLink(control: FormControl): {[key: string]: boolean} {
        if(/^https:\/\/github.com\//.test(control.value) || /^http:\/\/github.com\//.test(control.value)){
              return null
         } else {
             return {restrictedGitHubLink: true}
         }
     }

}