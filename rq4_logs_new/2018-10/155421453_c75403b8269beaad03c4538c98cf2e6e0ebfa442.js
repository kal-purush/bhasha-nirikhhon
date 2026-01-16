


const COMMENT_START = /(^([ \t]+)?\/\*|^([ \t]+)?###)/gi;

const COMMENT_END = /(\*\/|^([ \t]+)?###)/gi;

const COMMENT_SYNTAX = /(^[ \t]+\/\* ?|^[ \t]+#(##)? ?| ?\*\/|^[ \t]+|[ \t]+$)/gi;

const COMMENT_RE = /\/\*[\s\S]*?\*\/|([^\\:]|^)\/\/.*$/gm;

var keepcomment = [];

function print(xhrtext){
  var receive = JSON.parse(xhrtext);
  console.log(receive);

}


class Line {

    /**
     * @public
     * @constructs Line
     * @param {Node} node A single line in a file.
     * @param {Number} index The index of the line in the file.
     */
    constructor(node, index) {
      this.node = node;
      this.index = index;
      this.text = node.textContent;
      this.parentNode = node.parentNode;
    }
  
    /**
     * Render line (adds comment style).
     * @public
     * @instance
     * @method render
     */
    render() {
      this.parentNode.classList.add('blob-expanded');
    }
  
    /**
     * Show line.
     * @public
     * @instance
     * @method show
     */
    show() {
      this.parentNode.style.display = '';
    }
  
    /**
     * Hide line.
     * @public
     * @instance
     * @method hide
     */
    hide() {
      this.parentNode.style.display = 'none';
    }
  
    /**
     * Cleanup and destroy line.
     * @public
     * @instance
     * @method destroy
     */
    destroy() {
      delete this.node;
      delete this.index;
      delete this.text;
      delete this.parentNode;
    }
  }

class Comment {

    /**
     * @public
     * @instance
     * @method constructor
     * @param {Line[]} lines - The lines of the comment.
     */
    constructor(lines) {
      this.lines = lines;
      this.length = this.lines.length;
      this.start = this.lines[0];
      this.end = this.lines[this.length - 1];
    }
  
    /**
     * Render the comment (creating a toggle).
     * @public
     * @instance
     * @method render
     */
    render() {
      this.lines.forEach(line => line.render());
    }
  
    /**
     * Show the comment.
     * @public
     * @instance
     * @method show
     */
    show() {
      this.lines.forEach(line => line.show());
  
    }
  
    /**
     * Hide the comment.
     * @public
     * @instance
     * @method hide
     */
    hide() {
      this.lines.forEach(line => line.hide());
  
    }
  
    /**
     * Cleanup and destroy comment.
     * @public
     * @instance
     * @method destroy
     */
    destroy() {
      delete this.lines;
      delete this.start;
      delete this.end;
      delete this.toggle;
    }
  }
  
  /**
   * @public
   * @class File
   */
  class File {
  
    /**
     * @public
     * @instance
     * @method constructor
     */
    constructor() {
      this.nodes = document.getElementsByClassName('blob-code');
      this.lines = [];
      this.comments = [];
      this.length = 0;
      this.parse();
      this.render();
      //this.show2();
      this.send();
    }
  
    /**
     * Break down lines of a file into comments.
     * @public
     * @instance
     * @method parse
     */
    parse() {
      var comment = false;
      var lines;
      this.length = this.nodes.length;
      
  
      for (var index = 0, length = this.nodes.length; index < length; index++) {
        var node = this.nodes[index];
        var line = new Line(node, index);
        keepcomment.push(line.text + " newlinecom ");
  
        this.lines.push(line);
  
        if (!comment && COMMENT_START.test(line.text)) {
          lines = [];
          comment = true;
        }
  
        if (comment) {
          lines.push(line);
        }
  
        if (comment && COMMENT_END.test(line.text)) {
          this.comments.push(new Comment(lines));
          comment = false;
        }
      }
    }
  
    /**
     * Render the files comments.
     * @public
     * @instance
     * @method show
     */
    render() {
      this.comments.forEach(comment => comment.render());
    }
  
    /**
     * Show file comments.
     * @public
     * @instance
     * @method hide
     */
    show() {
      this.comments.forEach(comment => comment.show());
      
    }

    show2()
    {
        
        let newWindow = window.open("about:blank", "", "_blank");
        if (newWindow) {
                this.keepcomment.forEach(function(textincom) {
              newWindow.document.write(JSON.stringify(textincom));
            });
        }
    }
    send()
    {
     
      }

    
    
  
    /**
     * Hide file comments.
     * @public
     * @instance
     * @method hide
     */
    hide() {
      var lineNum = +location.hash.replace('#L', '');
  
      this.comments.forEach(comment => {
        if (lineNum <= comment.start.index || lineNum >= comment.end.index) {
          comment.hide();
        }
      });
    }
  
    /**
     * Cleanup and destroy file.
     * @public
     * @instance
     * @method destroy
     */
    destroy() {
      this.comments.forEach(comment => comment.destroy());
      this.lines.forEach(line => line.destroy());
      delete this.nodes;
      delete this.lines;
      delete this.comments;
    }
  }

class Extension {

    /**
     * @public
     * @instance
     * @method constructor
     */
    constructor() {
      this.container = document.getElementById('js-repo-pjax-container');
      this.render();
      this.observe();
    }
  
    /**
     * Creates a MutationObserver that watches for the file viewer to be swapped
     * out.
     * @public
     * @instance
     * @method observe
     */
    observe() {
      this.observer = new MutationObserver(() => {
        this.destroy();
        this.render();

      });
      this.observer.observe(this.container, {
        childList: true
      });
    }

  
    /**
     * Renders the extension
     * @public
     * @instance
     * @method render
     */
    render() {
      this.file = new File();
     
  
    }
  
    /**
     * Cleanup and destroy extension.
     * @public
     * @instance
     * @method destroy
     */
    
    destroy() {
      this.file.destroy();
      delete this.file;
 
    }
  }
  
  /**
   * Kick off extension.
   * @type {Extension}
   */
  
  (() => new Extension())();

  const API_URL = 'http://127.0.0.1:5000/';
  const input = JSON.stringify(keepcomment);

  const xhr = new XMLHttpRequest();
  const data = new FormData();
  
  data.append('input', input);

  xhr.open('POST', API_URL, true);

  xhr.onreadystatechange = function () {
    if(xhr.readyState == 4) {
      console.log("Hi")
      console.log(xhr.status)
      if(xhr.status == 200 || xhr.status == 0)
      {
      console.log(xhr.responseText);
      var receiveJson = JSON.parse(xhr.responseText);
      console.log(receiveJson['comment'])
      
      var i;
      for (i = 1; i < 2; i++) { 
        text = "LC"+ i.toString();
        document.getElementById(text).style.backgroundColor = "orange";
        document.getElementById(text).style.color = "black";
      }
     
     
      }
   

     
    }
    else{
      console.log("Hello");
    }
    
  };

  xhr.send(data);


 
 
  
  