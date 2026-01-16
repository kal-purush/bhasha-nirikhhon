import { Component, Input, OnInit } from '@angular/core';
import { Observable } from 'rxjs';
import { wowAchievement } from '../../interfaces/achievement';
import { GlobalStoreService } from '../../services/global-store.service';

@Component({
  selector: 'app-wow-achievement-lazy',
  templateUrl: './wow-achievement-lazy.component.html',
  styleUrls: ['./wow-achievement-lazy.component.scss']
})
export class WowAchievementLazyComponent implements OnInit {

  @Input() av!: wowAchievement;
  @Input() descr!: string;

  public mobile$: Observable<boolean>;


  constructor(private globalStore: GlobalStoreService) {
    this.mobile$ = this.globalStore.mobile$;
  }

  ngOnInit(): void {
  }

}