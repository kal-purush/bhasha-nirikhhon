import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';
import {environment} from '../../../environments/environment';
import {MessageResponseInterface} from '../types/messageResponse.interface';

@Injectable({
    providedIn: 'root'
})
export class ImageMetringPointService {

    constructor(private http: HttpClient) {
    }

    uploadImageToControlReading(controlReadingId: string, formData: FormData): Observable<MessageResponseInterface> {
        const url = environment.apiUrl + '/image/' + controlReadingId + '/upload';
        return this.http.post<MessageResponseInterface>(url, formData);
    }
}