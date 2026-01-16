import {Injectable} from "angular2/core";
import {CONTACTS} from "./mock-contact";

@Injectable()
export class ContactService {
	getContacts() {
		return Promise.resolve(CONTACTS)
	}
}