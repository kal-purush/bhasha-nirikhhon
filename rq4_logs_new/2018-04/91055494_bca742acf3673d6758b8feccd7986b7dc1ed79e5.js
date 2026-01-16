class Bingo {
  constructor(){
    this.stack = [];
    for(let i=1;i<=75;i++){
        this.stack.push(i);
    }
    this.is_doing = false;
    this.audio = $('#audio').get(0);
    try {
      this.audio.play();
    } catch (e){
      console.log('cannot play audio');
    }
    this.result_block = $('#js-result');
    this.result_tr = this.result_block;
    this.result_count = 0;
  }
  play(){
    if(this.is_doing == true) return;
    let stack_length = this.stack.length;
    if(stack_length == 0){
        this.audio.pause();
        return;
    }
    this.is_doing = true;
    let open = $('#js-open');
    let count = 0;
    let timer = setInterval(() => {
      let tmp_rand = Math.floor( Math.random() * stack_length);
      open.html(this.stack[tmp_rand]);
      if(count >= 20){
          clearInterval(timer);
          var rand = Math.floor( Math.random() * stack_length);
          open.html(this.stack[rand]);
          if(this.result_count%30 == 0){
            this.result_block.append('<div id="js-result_tr' + this.result_count + '" style="display:table-row"></div>');
            this.result_tr = $('#js-result_tr' + this.result_count);
          }
          this.result_count ++;
          this.result_tr.append('<div class="result">' + this.stack[rand] + '</div>');
          this.stack.splice(rand,1);
          if(this.stack.length === 0){
            try {
              this.audio.pause();
              } catch (e){
                console.log('cannot pause audio');
              }
          }
          this.is_doing = false;
      }
      count++;
    },100);
  }
};

$(document).ready(function(){
  var bingo = new Bingo();
  $('body,.container').on("click keydown ontouchstart",function(){
    bingo.play();
  });
});