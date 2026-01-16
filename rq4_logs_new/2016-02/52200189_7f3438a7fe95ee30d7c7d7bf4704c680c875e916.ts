//xxx <xreference path="../node_modules/csv-parse/lib/index.js"/>

import {bootstrap} from 'angular2/platform/browser';
import {Component, Pipe, PipeTransform} from 'angular2/core';
import {NgFor} from 'angular2/common';
//import {AlaSQL} from 'alasql/dist/alasql';

@Pipe({name: 'byteFormat'})
class ByteFormatPipe implements PipeTransform {
    // Credit: http://stackoverflow.com/a/18650828
    transform(bytes, args) {
        if (bytes == 0) return '0 Bytes';
        var k = 1000;
        var sizes = ['Bytes', 'KB', 'MB', 'GB'];
        var i = Math.floor(Math.log(bytes) / Math.log(k));
        return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i];
    }
}

@Component({
    selector: 'app',
    pipes: [ByteFormatPipe],
    template: `

    <h1>Total Items: {{ getData().length }}</h1>
    <h1>Total Size: {{ imageStats().size | byteFormat}}</h1>

    <div 
      (dragover)="false" 
      (dragend)="false" 
      (drop)="handleDrop($event)"
      style="height: 300px; border: 5px dotted #ccc;">
      <p style="margin: 10px; text-align: center">
        <strong>Drop Your Images Here</strong>
      </p>
    </div>

    <div class="media" *ngFor="#image of images">
      <div class="media-left">
        <a href="#">
          <img class="media-object" src="{{image.path}}" style="max-width:200px">
        </a>
      </div>
      <div class="media-body">
        <h4 class="media-heading">{{image.name}}</h4>
        <p>{{image.size | byteFormat}}</p>
      </div>
    </div>
  `
})

export class App {

    images:Array<Object> = [];
    data:Array<Object> = [];

    constructor() {
        var fs=require("fs");
        //var transform = require('stream-transform');
        //var sql=require("sqlite3");

        var alasql=require("alasql");
        alasql("CREATE TABLE line (date string, type string, description string, amount number)");

        var file = "midata2438.csv";
        var input="#"+fs.readFileSync(file);

        var csv=require("csv-parse");

        csv(input, {comment: '#'}, function(err, output) {
            this.data=output;
            output.forEach(function(line) {
                console.log(line);
                if ( line.length < 4 ) {
                    return;
                }
                var amount = Number(line[3].replace(/[^0-9\.]+/g,""));
                var cols=`'${line[0]}','${line[1]}','${line[2].replace("'", "\\'")}',${amount}`;
                alasql("INSERT INTO line VALUES ("+cols+")");
            });
        });


        //console.log(AlaSQL);
        //var db = new sqlite3.Database(file);
    }

    handleDrop(e) {
        var files:File = e.dataTransfer.files;
        var self = this;
        Object.keys(files).forEach((key) => {
            if (files[key].type === "image/png" || files[key].type === "image/jpeg") {
                self.images.push(files[key]);
            }
            else {
                alert("File must be a PNG or JPEG!");
            }
        });

        return false;
    }

    getData() {
        return this.data;
    }

    imageStats() {

        let sizes:Array<number> = [];
        let totalSize:number = 0;

        this
            .images
            .forEach((image:File) => sizes.push(image.size));

        sizes
            .forEach((size:number) => totalSize += size);

        return {
            size: totalSize,
            count: this.images.length
        }

    }

}

bootstrap(App);