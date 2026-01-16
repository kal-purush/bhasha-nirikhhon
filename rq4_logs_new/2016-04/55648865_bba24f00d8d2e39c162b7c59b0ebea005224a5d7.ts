import {Component} from 'angular2/core';

@Component({
    selector: 'app',
    template: `
    <section class="setup">
    <h2>Game Setup</h2>
    Enter your name please:
    <input type="text">
    </section>
    <section>
      <h2>The Puzzle</h2>
      <p>Okay, welcome <span class="name">XXX</span></p>
      <br>
      <br>
      Switch 1:
      <input type="text"><br>
      Switch 2:
      <input type="text"><br>
      Switch 3:
      <input type="text"><br>
      Switch 4:
      <input type="text"><br>
    </section>
    <h2>Congratulations XXX, you did it!</h2>
    `,
})
export class AppComponent {

}