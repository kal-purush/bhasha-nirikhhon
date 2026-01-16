import { Component } from '@angular/core';
import { AbstractControl, FormControl, FormGroup, ValidationErrors, Validators } from '@angular/forms';
import { MAX_MESSAGE_COUNT, MAX_MESSAGE_LENGTH, MIN_MESSAGE_COUNT, MIN_MESSAGE_LENGTH } from '../config';
import { Message } from '../model/message';
import { MESSAGE_CSV_EMAIL_COLUMN, MESSAGE_CSV_OBLIGATORY_COLUMNS, MESSAGE_CSV_OPTIONAL_COLUMNS, MessageCsvModel } from './../model/message-csv.model';

interface ErrorMessage {
  rowNumber?: number;
  errorReason: string;
}

@Component({
  selector: 'csv',
  templateUrl: './csv.component.html',
  styleUrl: './csv.component.css'
})
export class CsvComponent {
  csvForm: FormGroup = new FormGroup({
    csvFile: new FormControl<string>('', [Validators.required]),
    csvContent: new FormControl<string[]>([], [this.csvContentValidator()]),
  })

  errorMessages: ErrorMessage[] = [];
  private readonly obligatoryColumns = MESSAGE_CSV_OBLIGATORY_COLUMNS.map( col => col.toLocaleLowerCase());
  private readonly optionalColumns = MESSAGE_CSV_OPTIONAL_COLUMNS.map( col => col.toLocaleLowerCase());
  private readonly emailColumnName = MESSAGE_CSV_EMAIL_COLUMN.toLocaleLowerCase();
  private readonly allColumns = [...this.optionalColumns, ...this.obligatoryColumns];

  constructor() {}

  onFileChange(event: any): void {
    const file = (event.target as HTMLInputElement).files?.[0];
    if(file) {
      const reader = new FileReader();  
      reader.readAsText(file as Blob);

      reader.onload = () => {  
        const csvData = reader.result as string;
        const csvRecordsArray = csvData.split(/\r\n|\n/);  

        if(csvRecordsArray?.length) {
          this.csvForm.get('csvContent').setValue(csvRecordsArray)
        }
      }; 
    }
  }

  getCsvFileColumnNames(): string {
    return this.allColumns.join(',');
  }

  onSubmit(): void {
    const csvContent = this.csvForm.get('csvContent')?.value as unknown as string[];
    const parsedMessages = this.getMessages(csvContent);

    const messages =  parsedMessages.map( message => {
      const order = message['Order'] ? Number(message['Order']) : null;
      return new Message(message['Message'], message['Email recepient'], order);
    } )

    console.log('File uploaded:', messages);

    this.removeSelectedFile();
  }

  private getHeaders(firstRow: string): string[] {  
    if(firstRow?.length) {
      const headers = firstRow.split(',');  
      const headerArray = [];  
      for (let j = 0; j < headers.length; j++) {  
        headerArray.push(headers[j]);  
      }  
      return headerArray;  
    }
    return [];
  }  

  private parseCsvFile(csvRecordsArray: string[], headersRow: string[]): MessageCsvModel[] {
    const messages: MessageCsvModel[] = [];  
  
    for (let i = 1; i < csvRecordsArray.length; i++) {  
      const curruntRecord = (<string>csvRecordsArray[i]).split(',');  
      let columnCounter = 0;
      let rowCounter = 1;

      const message: MessageCsvModel = {
        Message: ''
      };

      headersRow.forEach((columnName) => {
        if(curruntRecord[columnCounter]?.length) {;
          message[columnName as keyof MessageCsvModel] = curruntRecord[columnCounter]?.trim();
          rowCounter++;
        }
        columnCounter++;
      })

      if(Object.values(message).length && message?.['Message'].length >= 1) {
        messages.push(message);
      }
    }  

    return messages;  
  }

  private getMessages(csvRecordsArray: string[]): MessageCsvModel[] {  
    const headersRow = this.getHeaders(csvRecordsArray?.[0]);
    if(!headersRow?.length) {
      return [];
    }

    return this.parseCsvFile(csvRecordsArray, headersRow);
  }  
  private csvContentValidator() {
    return (control: AbstractControl): ValidationErrors => {
      const csvContent: string[] = control?.value;
      this.errorMessages = [];

      if(!csvContent.length) {
        return null;
      }

      const headers = this.getHeaders(csvContent?.[0])?.map( header => header?.toLocaleLowerCase());

      if(!headers?.length) {
        this.createErrorMessage(`Uploaded file is empty`);
        return { invalidCsvContent: true };
      }

      if(!this.obligatoryColumns.every((requiredHeader) => headers.includes(requiredHeader))) {
        this.createErrorMessage(`CSV file must contains the following obligatory headers: ${this.obligatoryColumns.join(', ')}`);
        return { invalidCsvContent: true };
      }

      if(headers.length > this.allColumns.length || !headers.every((providedHeader) => this.allColumns.includes(providedHeader as keyof MessageCsvModel))) {
        this.createErrorMessage(`Required columns: ${this.obligatoryColumns.join(', ')}, allowed columns: ${this.optionalColumns.join(', ')}`);
        return { invalidCsvContent: true };
      }

      const messagesOnly = csvContent.slice(1, csvContent.length - 1);

      if(messagesOnly?.length > MAX_MESSAGE_COUNT || messagesOnly?.length < MIN_MESSAGE_COUNT) { // max message numbers + headers row
        this.createErrorMessage(`Message count must be between ${MIN_MESSAGE_COUNT} and ${MAX_MESSAGE_COUNT}`);
        return { invalidCsvContent: true };
      }

      messagesOnly.map( (row, rowIndex) => {
        
        const rowNumber = rowIndex + 1;

        const record = row?.split(',');

        if(record?.length) {
          record.map( (cell, cellIndex) => {
            const columnName = headers[cellIndex]?.toLocaleLowerCase() as keyof MessageCsvModel;

            if(this.obligatoryColumns.includes(columnName) && (cell?.length < MIN_MESSAGE_LENGTH || cell?.length > MAX_MESSAGE_LENGTH)) {
              this.createErrorMessage(`Message length must be between ${MIN_MESSAGE_LENGTH} and ${MAX_MESSAGE_LENGTH}`, rowNumber);
            }
            if(columnName === this.emailColumnName && !cell.includes('@') && cell?.length) {
              this.createErrorMessage(`Invalid email format: ${cell}`, rowNumber);
            }
          })
        }
      } )

      if (this.errorMessages.length) {
        return { invalidCsvContent: true };
      }
  
      return null;
    };
  }

  removeSelectedFile(): void {
    this.csvForm.get('csvContent')?.setValue([]);
    this.csvForm.get('csvFile')?.setValue('');
    this.errorMessages = [];
  }

  private createErrorMessage(errorReason: string, rowNumber?: number) {
    const error: ErrorMessage = {
      errorReason: errorReason
    }
    if(rowNumber) {
      error.rowNumber = rowNumber
    }
    this.errorMessages.push(error)
  }
}