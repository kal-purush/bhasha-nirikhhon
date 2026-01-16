import {Component, AfterViewInit, OnInit, HostListener} from '@angular/core'

@Component({
  selector: 'my-app',
  template: `
    <div id="book">
      <canvas id="pageflip-canvas"></canvas>
      <div id="pages">
        <section *ngFor="let item of contents">
          <h2>{{item.title}}</h2>
          <div>
            <h2>{{item.subtitle}}</h2>
            <p>{{item.section}}</p>
          </div>
        </section>
      </div>
    </div>
    `,
    styles: [`
    #book {
      background: url("book.png") no-repeat;
      position: absolute;
      width: 830px;
      height: 520px;
      left: 50%;
      top: 50%;
      margin-left: -400px;
      margin-top: -250px;
    }
    #pages section {
      background: url("paper.png") no-repeat;
      display: block;
      width: 400px;
      height:  500px;
      position: absolute;
      left: 415px;
      top: 5px;
      overflow: hidden;
    }
    #pages section>div {
      display: block;
      width: 400px;
      height: 500px;
      font-size: 12px;
    }
    #pages section p,
    #pages section h2 {
      padding: 3px 35px;
      line-height: 1.4em;
      text-align: justify;
    }
    #pages section h2{
      margin: 15px 0 10px;
    }

    #pageflip-canvas {
      position: absolute;
      z-index: 100;
    }
  `]
})


export class AppComponent implements OnInit, AfterViewInit {

  private contents: any;

  // Dimensions of the whole book
  private BOOK_WIDTH = 830;
  private BOOK_HEIGHT = 520;
  
  // Dimensions of one page in the book
  private PAGE_WIDTH = 400;
  private PAGE_HEIGHT = 500;
  
  // Vertical spacing between the top edge of the book and the papers
  private PAGE_Y = ( this.BOOK_HEIGHT - this.PAGE_HEIGHT ) / 2;
  
  // The canvas size equals to the book dimensions + this padding
  private CANVAS_PADDING = 60;
  
  private page = 0;
  
  private mouse = { x: 0, y: 0 };

  private flips = [];

  private canvas: HTMLCanvasElement;
  private context: CanvasRenderingContext2D;
  private book: HTMLElement;
  private pages: any;

  constructor () {
    this.contents = [{
      title: 'ページ１',
      subtitle: '',
      section: '最近は、ワールド ワイド ウェブを使って世界中の情報を簡単に入手できるようになりました。世界中のいろいろな人と知り合うこともできるし、他の国の出来事も瞬時に伝わってきます',
    },{
      title: 'ページ２',
      subtitle: 'ページ２',
      section: '最近は、ワールド ワイド ウェブを使って世界中の情報を簡単に入手できるようになりました。世界中のいろいろな人と知り合うこともできるし、他の国の出来事も瞬時に伝わってきます',
    },{
      title: '20thingsilearned.com',
      subtitle: '',
      section: '<a href="http://www.html5rocks.com/ja/tutorials/casestudies/20things_pageflip/">20thingsilearned.com</a>',
    }];
  }

  ngOnInit() {
  	this.canvas = <HTMLCanvasElement>document.getElementById("pageflip-canvas");
    this.context = this.canvas.getContext( "2d" );

    this.book = document.getElementById( "book" );
    
    // List of all the page elements in the DOM
    this.pages = this.book.getElementsByTagName( "section" );
  }

  ngAfterViewInit () {
    // Organize the depth of our pages and create the flip definitions
    for( let i = 0, len = this.pages.length; i < len; i++ ) {
      this.pages[i].style.zIndex = String(len - i);
      
      this.flips.push({
        // Current progress of the flip (left -1 to right +1)
        progress: 1,
        // The target value towards which progress is always moving
        target: 1,
        // The page DOM element related to this flip
        page: this.pages[i], 
        // True while the page is being dragged
        dragging: false
      });
    }
    
    // Resize the canvas to match the book size
    this.canvas.width = this.BOOK_WIDTH + ( this.CANVAS_PADDING * 2 );
    this.canvas.height = this.BOOK_HEIGHT + ( this.CANVAS_PADDING * 2 );
    
    // Offset the canvas so that it's padding is evenly spread around the book
    this.canvas.style.top = -this.CANVAS_PADDING + "px";
    this.canvas.style.left = -this.CANVAS_PADDING + "px";

    // Render the page flip 60 times a second
    setInterval( this.render, 1000 / 60 );
  }

  @HostListener('mousemove', ['$event'])
  private mouseMoveHandler = (event) => {
    // Offset mouse position so that the top of the book spine is 0,0
    this.mouse.x = event.clientX - this.book.offsetLeft - ( this.BOOK_WIDTH / 2 );
    this.mouse.y = event.clientY - this.book.offsetTop;
  }
  
  @HostListener('mousedown', ['$event'])
  private mouseDownHandler = ( event ) => {
    // Make sure the mouse pointer is inside of the book
    if (Math.abs(this.mouse.x) < this.PAGE_WIDTH) {
      if (this.mouse.x < 0 && this.page - 1 >= 0) {
        // We are on the left side, drag the previous page
        this.flips[this.page - 1].dragging = true;
      }
      else if (this.mouse.x > 0 && this.page + 1 < this.flips.length) {
        // We are on the right side, drag the current page
        this.flips[this.page].dragging = true;
      }
    }
    
    // Prevents the text selection
    event.preventDefault();
  }
  
  @HostListener('mouseup', ['$event'])
  private mouseUpHandler = ( event ) => {
    for( let i = 0; i < this.flips.length; i++ ) {
      // If this flip was being dragged, animate to its destination
      if( this.flips[i].dragging ) {
        // Figure out which page we should navigate to
        if( this.mouse.x < 0 ) {
          this.flips[i].target = -1;
          this.page = Math.min( this.page + 1, this.flips.length );
        }
        else {
          this.flips[i].target = 1;
          this.page = Math.max( this.page - 1, 0 );
        }
      }
      
      this.flips[i].dragging = false;
    }
  }

  private render = () => {
    
    // Reset all pixels in the canvas
    this.context.clearRect( 0, 0, this.canvas.width, this.canvas.height );
    
    for( let i = 0, len = this.flips.length; i < len; i++ ) {
      let flip = this.flips[i];
      
      if( flip.dragging ) {
        flip.target = Math.max( Math.min( this.mouse.x / this.PAGE_WIDTH, 1 ), -1 );
      }
      
      // Ease progress towards the target value 
      flip.progress += ( flip.target - flip.progress ) * 0.2;
      
      // If the flip is being dragged or is somewhere in the middle of the book, render it
      if( flip.dragging || Math.abs( flip.progress ) < 0.997 ) {
        this.drawFlip( flip );
      }
    }
  }
  
  private drawFlip = (flip) => {
    // Strength of the fold is strongest in the middle of the book
    let strength = 1 - Math.abs( flip.progress );
    
    // Width of the folded paper
    let foldWidth = ( this.PAGE_WIDTH * 0.5 ) * ( 1 - flip.progress );
    
    // X position of the folded paper
    let foldX = this.PAGE_WIDTH * flip.progress + foldWidth;
    
    // How far the page should outdent vertically due to perspective
    let verticalOutdent = 20 * strength;
    
    // The maximum width of the left and right side shadows
    let paperShadowWidth = ( this.PAGE_WIDTH * 0.5 ) * Math.max( Math.min( 1 - flip.progress, 0.5 ), 0 );
    let rightShadowWidth = ( this.PAGE_WIDTH * 0.5 ) * Math.max( Math.min( strength, 0.5 ), 0 );
    let leftShadowWidth = ( this.PAGE_WIDTH * 0.5 ) * Math.max( Math.min( strength, 0.5 ), 0 );
    
    
    // Change page element width to match the x position of the fold
    flip.page.style.width = Math.max(foldX, 0) + "px";
    
    this.context.save();
    this.context.translate( this.CANVAS_PADDING + ( this.BOOK_WIDTH / 2 ), this.PAGE_Y + this.CANVAS_PADDING );
    
    
    // Draw a sharp shadow on the left side of the page
    this.context.strokeStyle = 'rgba(0,0,0,'+(0.05 * strength)+')';
    this.context.lineWidth = 30 * strength;
    this.context.beginPath();
    this.context.moveTo(foldX - foldWidth, -verticalOutdent * 0.5);
    this.context.lineTo(foldX - foldWidth, this.PAGE_HEIGHT + (verticalOutdent * 0.5));
    this.context.stroke();
    
    
    // Right side drop shadow
    let rightShadowGradient = this.context.createLinearGradient(foldX, 0, foldX + rightShadowWidth, 0);
    rightShadowGradient.addColorStop(0, 'rgba(0,0,0,'+(strength*0.2)+')');
    rightShadowGradient.addColorStop(0.8, 'rgba(0,0,0,0.0)');
    
    this.context.fillStyle = rightShadowGradient;
    this.context.beginPath();
    this.context.moveTo(foldX, 0);
    this.context.lineTo(foldX + rightShadowWidth, 0);
    this.context.lineTo(foldX + rightShadowWidth, this.PAGE_HEIGHT);
    this.context.lineTo(foldX, this.PAGE_HEIGHT);
    this.context.fill();
  
    // Left side drop shadow
    let leftShadowGradient = this.context.createLinearGradient(foldX - foldWidth - leftShadowWidth, 0, foldX - foldWidth, 0);
    leftShadowGradient.addColorStop(0, 'rgba(0,0,0,0.0)');
    leftShadowGradient.addColorStop(1, 'rgba(0,0,0,'+(strength*0.15)+')');
    
    this.context.fillStyle = leftShadowGradient;
    this.context.beginPath();
    this.context.moveTo(foldX - foldWidth - leftShadowWidth, 0);
    this.context.lineTo(foldX - foldWidth, 0);
    this.context.lineTo(foldX - foldWidth, this.PAGE_HEIGHT);
    this.context.lineTo(foldX - foldWidth - leftShadowWidth, this.PAGE_HEIGHT);
    this.context.fill();
    
    // Gradient applied to the folded paper (highlights & shadows)
    let foldGradient = this.context.createLinearGradient(foldX - paperShadowWidth, 0, foldX, 0);
    foldGradient.addColorStop(0.35, '#fafafa');
    foldGradient.addColorStop(0.73, '#eeeeee');
    foldGradient.addColorStop(0.9, '#fafafa');
    foldGradient.addColorStop(1.0, '#e2e2e2');
    
    this.context.fillStyle = foldGradient;
    this.context.strokeStyle = 'rgba(0,0,0,0.06)';
    this.context.lineWidth = 0.5;
    
    // Draw the folded piece of paper
    this.context.beginPath();
    this.context.moveTo(foldX, 0);
    this.context.lineTo(foldX, this.PAGE_HEIGHT);
    this.context.quadraticCurveTo(foldX, this.PAGE_HEIGHT + (verticalOutdent * 2), foldX - foldWidth, this.PAGE_HEIGHT + verticalOutdent);
    this.context.lineTo(foldX - foldWidth, -verticalOutdent);
    this.context.quadraticCurveTo(foldX, -verticalOutdent * 2, foldX, 0);
    
    this.context.fill();
    this.context.stroke();

    this.context.restore();
  }
}
　