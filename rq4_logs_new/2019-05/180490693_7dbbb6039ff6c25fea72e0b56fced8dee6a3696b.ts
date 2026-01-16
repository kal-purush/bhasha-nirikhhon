import { AbstractControl } from "@angular/forms";
import { Observable, Observer, of } from "rxjs";

export const mimeType = (control: AbstractControl): Promise<{ [key: string]: any }> | Observable<{ [key: string]: any }> => {
    if (typeof control.value === 'string') {
        return of(null);
    }
    const file = control.value as File;
    const fileReader = new FileReader();
    return Observable.create((observer: Observer<{ [key: string]: any }>) => {
        fileReader.addEventListener("loadend", () => {
            const arr = new Uint8Array(<any>fileReader.result).subarray(0, 4);
            let header = '';
            let isValid = false;
            arr.forEach(num => {
                header += num.toString(16);
            })
            switch(header) {
                case "89504e47": isValid = true; break;
                case "ffd8ffe0":           
                case "ffd8ffe1":
                case "ffd8ffe2":
                case "ffd8ffe3":
                case "ffd8ffe8": isValid = true; break;
                default: isValid = false; break;
            }
            if (isValid) {
                observer.next(null);
            } else {
                observer.next({ invalidMineType: true })
            }
            observer.complete();
        });
        fileReader.readAsArrayBuffer(file);
    })
}