import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Member } from 'modules/management/models/member'
import { MemberService } from '@modules/management/services/member.service';
import { AngularFireDatabase, AngularFireList, AngularFireObject } from '@angular/fire/database';
import { FormBuilder, FormGroup } from '@angular/forms';
@Component({
  selector: 'sb-enroll',
  templateUrl: './enroll.component.html',
  styleUrls: ['./enroll.component.scss']
})
export class EnrollComponent implements OnInit {
  studentsRef: AngularFireList<any> | undefined;
  studentRef: AngularFireObject<any> | undefined;

  public membersForm: FormGroup|any;

  constructor(public router: Router,public membercrud: MemberService, public fb: FormBuilder) { }

  ngOnInit(): void {
    this.memberForm();
    console.log(this.membercrud.getAll())
  }
 
  memberForm() {
    this.membersForm = this.fb.group({
      number: [''],
      name: [''],
      working: [''],
      work_sdate: [''],
      work_edate: [''],
      birth_date: [''],
      break_cnt: [''],
      break_cnt2: [''],
      position: [''],
      extension: [''],
      card_num: [''],
      tel: [''],
      phone: [''],
      email: [''],
      post: [''],
      addr1: [''],
      addr2: [''],
      img: [''],
      comment: [''],
      sign_img: [''],
      break_use_cnt: [''],
      level:[''],
      modify: [''],
      checked: [''],
    })
  }
  get number() {
    return this.membersForm.get('number')
  }

  get name() {
    return this.membersForm.get('name')
  }

  get working() {
    return this.membersForm.get('working')
  }
  get work_sdate() {
    return this.membersForm.get('work_sdate')
  }

  get work_edate() {
    return this.membersForm.get('work_edate')
  }

  get birth_date() {
    return this.membersForm.get('birth_date')
  }
  get break_cnt() {
    return this.membersForm.get('break_cnt')
  }

  get position() {
    return this.membersForm.get('position')
  }

  get extension() {
    return this.membersForm.get('extension')
  }
  get card_num() {
    return this.membersForm.get('card_num')
  }
  get tel() {
    return this.membersForm.get('tel')
  }

  get phone() {
    return this.membersForm.get('phone')
  }

  get email() {
    return this.membersForm.get('email')
  }
  get post() {
    return this.membersForm.get('post')
  }
  get addr1() {
    return this.membersForm.get('addr1')
  }
  get addr2() {
    return this.membersForm.get('addr2')
  }

  get img() {
    return this.membersForm.get('img')
  }

  get comment() {
    return this.membersForm.get('comment')
  }
  get sign_img() {
    return this.membersForm.get('sign_img')
  }
  get break_use_cnt() {
    return this.membersForm.get('break_use_cnt')
  }
  get level() {
    return this.membersForm.get('level')
  }
 
  ResetForm() {
    this.membersForm.reset();
  }

  golist() {
    this.router.navigateByUrl('/member')
  }

  save() {
    this.membercrud.AddMember(this.membersForm.value);
    this.router.navigateByUrl('/member')
  }

  Checks: Array<any> = [
		{ name: '와이즈빌1', details: [  '개발팀','영업팀', ] },
		{ name: '와이즈빌2', details: [  '홍보팀','행정팀',  ] }
	];
    
	details: Array<any> | undefined; 

	changeCheck(check: any) { 
		
		this.details = this.Checks.find((chk: any) => chk.name == check).details; //Angular 11
	}
  cnlth() {
    this.router.navigateByUrl('/managements/member')
  }

  AddStudent(student: Member) {

  }

}