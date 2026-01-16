import { Component, OnInit, Inject } from '@angular/core';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { ActivatedRoute } from '@angular/router';
import { DragulaService } from 'ng2-dragula';
import 'rxjs/add/operator/takeUntil';
import { Subject } from 'rxjs/Subject';
import { Validators, FormGroup, FormArray, FormBuilder } from '@angular/forms';
import { SceneService } from '../../services/scene.service';

@Component({
  selector: 'app-scene-actions-dialog',
  templateUrl: './scene-actions-dialog.component.html',
  styleUrls: ['./scene-actions-dialog.component.css']
})
export class SceneActionsDialogComponent implements OnInit {
  public myForm: FormGroup;
  scenes: any = this.data.scenes.slice();
  archiveActions: any = [];
  private destroy$ = new Subject();

  constructor(
    public dialogRef: MatDialogRef<SceneActionsDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any,
    private sceneService: SceneService,
    private _fb: FormBuilder,
    private dragulaService: DragulaService
  ) { }

  ngOnInit() {
    this._setScenes();
    this.myForm = this._fb.group({
      name: [this.data.name],
      scene_actions: this._fb.array([])
    });

    if (!this.data.scene_actions.length) {
      this.addAction();
    }

    for (let i=0; i<this.data.scene_actions.length; i++) {
      this.addAction(this.data.scene_actions[i]);
    }
  }

  _setScenes() {
    this.scenes.unshift({id: 'use_next', name: 'Use Next Scene'});
    this.scenes = this.scenes.filter(obj => obj.id !== this.data.id);
  }

  initAction(obj={}) {
    return this._fb.group({
      id: [obj['id']],
      name: [obj['name'], Validators.required],
      link_scene_id: [obj['link_scene_id'] || this.scenes[0].id, Validators.required],
    });
  }

  addAction(obj={}) {
    const control = <FormArray>this.myForm.controls['scene_actions'];
    control.push(this.initAction(obj));
  }

  removeAction(i: number) {
    this._handleArchivedAction(i);

    const control = <FormArray>this.myForm.controls['scene_actions'];
    control.removeAt(i);
  }

  _handleArchivedAction(i) {
    if (!!this.myForm.value.scene_actions[i].id) {
      let obj = this.myForm.value.scene_actions[i];
      obj['_destroy'] = true;

      this.archiveActions.push(obj);
    }
  }

  handleSubmit(): void {
    if (this.myForm.invalid) { return; }

    return this._update();
  }

  _update() {
    this.sceneService.update(this.data.story_id, this.data.id, this._buildData()).subscribe(
      res => {
        this.dialogRef.close(res);
      },
      err => {
        console.log("Error occured");
      }
    );
  }

  _buildData() {
    let actions = this.myForm.value.scene_actions.slice();
    let self = this;

    actions.map(function(action, index) {
      if (action.link_scene_id == 'use_next') {
        action.link_scene_id = null;
        action.use_next = true;
      }
      action.story_id = self.data.story_id;
      action.display_order = index + 1;
    })

    actions = actions.concat(this.archiveActions);

    let formData: FormData = new FormData();
    let obj = {
      scene: {
        id: this.data.id,
        scene_actions_attributes: actions,
      }
    };

    formData.append("data", JSON.stringify(obj));
    return formData;
  }

}