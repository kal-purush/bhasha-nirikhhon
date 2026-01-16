import {Component} from 'angular2/core';

import {BittenBuild} from './bittenbuild';
import {BittenStep} from './bittenstep';
import {BuildService} from './build_service';

@Component({
  selector: 'bitten-latest',
  template: `
  <div *ngIf="latestBuilds.length>0">
  <h3 class="builds"><a href="#">Latest Build</a> for {{latestBuilds[0].config}}</h3>
  <table class="builds">
    <tbody>
        <tr>
            <th>
                [{{latestBuilds[0].rev}}]
                <p class="date">{{latestBuilds[0].rev_time.toUTCString()}}</p>
            </th>
            <td *ngFor="#latestBuild of latestBuilds" class="{{latestBuild.status.toLowerCase()}}">
                    <p><a href="/builds/{{latestBuild.id}}">{{latestBuild.platform}}</a></p>
                    <p>{{latestBuild.started.toUTCString()}}</p>
                    <p>{{latestBuild.slave}}</p>
                    <p class="status"><b>{{latestBuild.status}}</b></p>
            </td>
        </tr>
    </tbody>
  </table>
  </div>`,
  providers: [BuildService]
})

export class BittenLatest{
    latestBuilds: BittenBuild[] = [];
    //latestBuilds: BittenBuild[] = [{id: 14, config: 'branches/0.6.x', platform: 'Linux', rev: 1574, rev_time: new Date("Mon, 21 Sep 2015 10:32:11 GMT"), slave: 'domu-12-31-39-0b-bd-44 (54.224.94.46)', started: new Date("Mon, 21 Sep 2015 12:00:00 GMT"), status: 'Success', stopped: new Date("Mon, 21 Sep 2015 12:21:45 GMT")}, {id: 14, config: 'branches/0.6.x', platform: 'Windows', rev: 1574, rev_time: new Date("Mon, 21 Sep 2015 10:32:11 GMT"), slave: 'domu-12-31-39-0b-bd-44 (54.224.94.46)', started: new Date("Mon, 21 Sep 2015 12:00:00 GMT"), status: 'Success', stopped: new Date("Mon, 21 Sep 2015 12:21:45 GMT")}];
    
    constructor(private _buildService: BuildService){
        this._buildService.getLatestBuilds().subscribe(data =>{
            this.latestBuilds = [];
            for(var i = 0; i<data.latestBuilds.length; i++){
                var build = new BittenBuild();
                build.id = data.latestBuilds[i].id;
                build.rev = data.latestBuilds[i].rev;
                build.rev_time = new Date(data.latestBuilds[i].rev_time);
                build.platform = data.latestBuilds[i].platform;
                build.config = data.latestBuilds[i].config;
                build.started = new Date(data.latestBuilds[i].started);
                build.stopped = new Date(data.latestBuilds[i].stopped);
                build.status = data.latestBuilds[i].status;
                build.slave = data.latestBuilds[i].slave;
                build.steps = [];
                this.latestBuilds.push(build);
            }
        });
    }
}
