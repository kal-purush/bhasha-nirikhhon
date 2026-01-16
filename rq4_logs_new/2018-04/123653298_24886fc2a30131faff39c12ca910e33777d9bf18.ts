import { Component, OnInit, Input } from '@angular/core';
import { Analysis } from '../models/analysis';
import { ActivatedRoute } from '@angular/router';
import { AnalysisService } from '../analysis.service';

@Component({
  selector: 'app-analysis-detail',
  templateUrl: './analysis-detail.component.html',
  styleUrls: ['./analysis-detail.component.css']
})
export class AnalysisDetailComponent implements OnInit {

  analysis: Analysis;

  private id: number;
  private name: string = '';
  private value: string = '';

  config = {
    "editable": true,
    "spellcheck": true,
    "height": "auto",
    "minHeight": "0",
    "width": "auto",
    "minWidth": "0",
    "translate": "yes",
    "enableToolbar": true,
    "showToolbar": true,
    "placeholder": "Enter text here...",
    "imageEndPoint": "",
    "toolbar": [
        ["bold", "italic", "underline", "strikeThrough", "superscript", "subscript"],
        ["fontName", "fontSize", "color"],
        ["justifyLeft", "justifyCenter", "justifyRight", "justifyFull", "indent", "outdent"],
        ["cut", "copy", "delete", "removeFormat", "undo", "redo"],
        ["paragraph", "blockquote", "removeBlockquote", "horizontalLine", "orderedList", "unorderedList"],
        ["link", "unlink", "image", "video"]
    ]
};

  constructor(private route: ActivatedRoute,  private analysisService: AnalysisService) { }

  ngOnInit() {
    this.getAnalysis();
  }

  submit() {
    this.analysisService.updateAnalysis(this.id, {
        id: this.id,
        priviledge: this.analysis.priviledge,
        name: this.name,
        value: this.value
    }).subscribe(analysis => {
      this.analysis = analysis;
      this.name = analysis.name;
      this.value = analysis.value;
    }, error => {
      console.log(error);
    })          
  }

  getAnalysis(): void {
    const id = +this.route.snapshot.paramMap.get('id');
    this.id = id;
    this.analysisService.getAnalysis(id)
      .subscribe(analysis => {
        this.analysis = analysis;
        this.name = analysis.name;
        this.value = analysis.value;
      }, error => {
        console.log(error);
      });
  }

}