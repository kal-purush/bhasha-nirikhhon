import { Component, OnInit, NgZone } from '@angular/core';
import { UserStory } from '../../objects/UserStory';
import { Directive, ElementRef } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { CustomValidators } from 'ng2-validation';

@Component({
	selector: 'app-project',
	templateUrl: './project.component.html',
	styleUrls: ['./project.component.css'],
})


export class ProjectComponent implements OnInit {
	private projectName : string = "MyProject";
	private addUsMode : boolean = false;
	private haveURL : boolean = false;

	private usList = [];
	private usModel= {
		id: 0 ,
		description: '',
		difficulty: 0,
		priority:0,
		status: '',
		onEdit: false
	};

	private project = {
		id:1,
		name: 'MyProject',
		description: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.',
		begin: '01/01/01',
		end:'12/12/21'
	}

	private idUS: number = 1;

	private repositoryForm: FormGroup;

	constructor(private el: ElementRef, private fb: FormBuilder, lc: NgZone) {
		this.repositoryForm = this.fb.group({
			url: [null, [Validators.required, CustomValidators.url]]
		});
	}

	ngOnInit() {
	}

	resetModel() {
		this.usModel.id=0;
		this.usModel.description='';
		this.usModel.difficulty=0;
		this.usModel.priority=0;
		this.usModel.status='';
		this.usModel.onEdit = false;
	}

	onEditRow(ligne) {
		ligne['onEdit'] = true;
		let tr_id = "#US"+ligne['id'];
		let tab = this.el.nativeElement.querySelectorAll(tr_id+" .editable");
		for (let i = 0; i < tab.length; ++i) {
			tab[i].classList.add("table-info");
  		tab[i].setAttribute('contenteditable','true');
		}
	}

	onBackRow(ligne) {
		ligne['onEdit'] = false;
		let tr_id = "#US"+ligne['id'];
		let tab = this.el.nativeElement.querySelectorAll(tr_id+" .editable");
		for (let i = 0; i < tab.length; ++i) {
			tab[i].classList.remove("table-info");
			tab[i].setAttribute('contenteditable','false');
		}
	}

	onConfirmRow(ligne) {
		ligne['onEdit'] = false;
		let tr_id = "#US"+ligne['id'];
		let tab = this.el.nativeElement.querySelectorAll(tr_id+" .editable");
		for (let i = 0; i < tab.length; ++i) {
			tab[i].classList.remove("table-info");
			tab[i].setAttribute('contenteditable','false');
		}
	}

	onDeleteRow(ligne) {
		let i = this.usList.indexOf(ligne);
		this.usList.splice(i,1);
	}

	onEdit() {
		this.addUsMode = true;
	}

	onBack() {
		this.addUsMode = false;
	}

	onConfirm() {
		this.addUsMode = false;
		this.usList.push(new UserStory(
			this.idUS,
			this.usModel.description,
			this.usModel.difficulty,
			this.usModel.priority,
			this.usModel.status
		));
		this.idUS++;
		this.resetModel();

	}

	submitURL() {
		this.haveURL = true;
	}

	editURL() {
		this.haveURL = false;
	}

	removeProject() {

	}

}