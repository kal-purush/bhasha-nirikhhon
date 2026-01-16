import {Injectable} from 'angular2/core';
import {History, User} from  './models/theModels';

@Injectable()
 export class HistoryFactory {
 
 
   createHistory() {
	   var h = new History();
		h.description = "Created";
		h.modificationDate = new Date(Date.now());
		h.modifiedBy = new User();
		h.modifiedBy.name = "Gud";
		h.modifiedBy.extId = "CyberDaDa";	   
	   return h;
 }
   
   
}