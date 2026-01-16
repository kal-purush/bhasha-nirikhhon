function Popup(options){
    this.modal = document.querySelector(options.modal);
    this.overlay = document.querySelector(options.overlay);
    
    var popup = this;
    
    this.open = function(content){
        popup.modal.innerHTML = content;
        popup.overlay.classList.add('open');
        popup.modal.classList.add('open');
    }
    
    this.close = function(){
        popup.overlay.classList.remove('open');
        popup.modal.classList.remove('open');
    }
    
    this.overlay.onclick = popup.close;
}

window.onload = function(){
    var p = new Popup({
        modal: '.modal',
        overlay: '.overlay'
    });
    
    document.querySelector('.write').onclick = function(){
        p.open('форма отправки сообщения');
    };

    document.querySelector('.callme').onclick = function(){
        var form = document.querySelector('.for-callme-popup');
        p.open(form.innerHTML);
    };
}