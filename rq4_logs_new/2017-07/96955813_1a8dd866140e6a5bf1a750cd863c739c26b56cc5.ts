import { Component, OnInit } from '@angular/core';

import { Router } from '@angular/router';

import { GamerInfoService } from '../../services/gamer-info/gamer-info.service';
import { GamerTagService } from '../../services/gamer-tag/gamer-tag.service';
import { SpinnerService } from '../../easy-spinner/services/spinner.service';

import { BungieApi } from '../../models/bungie.api.interface';

@Component({
    selector: 'app-sign-in',
    styleUrls: ['sign-in.component.scss'],
    templateUrl: './sign-in.component.html'
})

export class SignInComponent implements OnInit {
  constructor(private infoService: GamerInfoService,
              private tagService: GamerTagService,
              private spinner: SpinnerService,
              private router: Router
              ) {};

   ngOnInit() { }

   getGamer(gamer) {
    this.spinner.show();
    this.infoService.get(gamer).subscribe( (data: BungieApi) => {
      this.spinner.hide();
      this.saveId(gamer, data.Response);
      this.router.navigateByUrl(`menu/${gamer.network}/${data.Response}`);
    }, error => {
      this.spinner.hide();
      console.log(error)
    });
  }

  saveId(gamer, id) {
     gamer = Object.assign({}, gamer, { membershipId: id });
     this.tagService.set(gamer);
  }
}