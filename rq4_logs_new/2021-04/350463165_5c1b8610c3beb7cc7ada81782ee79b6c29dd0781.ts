import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { HR_REVIEW_SEND_URL } from '../../environments/environment';

export interface HrReview {
  review: string;
  englishLevel: string;
  technicalInterviewDate: string;
}

@Injectable({
  providedIn: 'root',
})
export class HrFormService {

  constructor(private http: HttpClient) { }

  sendHrFormData(hrReview: HrReview): Promise<void> {
    return new Promise<void>(() => {
      this.http.post(HR_REVIEW_SEND_URL, hrReview);
    });
  }
}