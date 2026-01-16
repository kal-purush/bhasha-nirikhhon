import { Component, OnInit } from '@angular/core';
import { DataService } from '../data.service';
import { LatLong } from '../lat-long';

@Component({
  selector: 'app-file-uploader',
  templateUrl: './file-uploader.component.html',
  styleUrls: ['./file-uploader.component.css']
})
export class FileUploaderComponent implements OnInit {

  constructor(private dataService: DataService) { }

  ngOnInit() {
  }

  foo(f: FileList) {
    const reader = new FileReader();
    reader.onloadend = ((ev: ProgressEvent) => {
      const results: { base: LatLong, targets: Array<LatLong> } = JSON.parse((<FileReader>(ev.target)).result);
      this.dataService.setBase(results.base);
      this.dataService.setTargets(results.targets);
    });
    reader.readAsText(f[0]);
  }

}