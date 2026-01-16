import { Injectable } from '@angular/core'
import * as tf from '@tensorflow/tfjs'
import * as $ from 'jquery'

@Injectable({
  providedIn: 'root'
})
export class MlServiceService {
  model;

  constructor() { }

  async ngOnInit() {
    console.log("Initialising")
    this.model = await tf.loadLayersModel('../../assets/models/model.json');  // load model
    console.log("Done initialising")
  };

  async predict(data: String) {
    console.log("Initialising")
    this.model = await tf.loadLayersModel('../../assets/models/model.json');  // load model
    console.log("Done initialising")
    console.log("Making prediction")
    const t = tf.tensor([[data]], [1]);
    t.print();
    let output = this.model.predict(t)
  }
}