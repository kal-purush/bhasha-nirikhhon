import { Component, OnInit, Input } from '@angular/core';
import {Participant} from './participant.model';
import {ParticipantService} from './requestForm.service';
import {SelectItem} from 'primeng/api';

@Component({
  selector: 'request-form',
  templateUrl: './requestForm.component.html'
})
export class RequestFormComponent implements OnInit {
    selectedpriorities:  SelectItem[];
    priority:Priority = {id:4};
    participants:Participant[] = [];
    frmParticipant:Participant = new Participant(null,null);
    constructor(private ps:ParticipantService) {

        this.selectedpriorities = [{label:'1',value:{id:1}},
            {label:'2',value:{id:2}},
            {label:'3',value:{id:3}},
            {label:'4',value:{id:4}}];
     }
    @Input()
    selectedValue: String = "YES" ;

  ngOnInit() {
    this.ps.getParticipants().subscribe(
        (data) => this.participants = data,
        (err) => console.log("Error",err)
    );
  }

  addParticipantToList(participant:Participant){
    // this.ps.addParticipant(this.frmParticipant).subscribe(
    //     (data) =>{
    //       console.log("Add Sucessful")
         this.participants.push(this.frmParticipant);
         this.frmParticipant = new Participant(null,null);
    //     }, (err) => {
    //       console.log("Add Error",err)
    //     }
    //   );
}
deleteParticipantFromList(name:String){
          let idx = this.participants.findIndex((e) =>e.name==name);
          this.participants.splice(idx,1);
       
}
}


interface Priority {
    id: number;
  }