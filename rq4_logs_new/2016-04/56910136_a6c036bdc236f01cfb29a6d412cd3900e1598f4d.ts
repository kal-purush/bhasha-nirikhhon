function fileChanged(e : any) {
    if (e.files != null && e.files.length == 0) {
        console.log('file not selected');
        return;
    }
    var file = e.files[0];
    var img = document.createElement('img');
    var fr = new FileReader();
    fr.onload = function () {
        img.src = this.result;
        img.onload = function () {
            var frame = document.querySelector('#crop_frame');
            frame.appendChild(this);
            var img = this;
            var top = 0;
            var left = 0;
            (() => {
                var startX;
                var startY;
                var clicked : boolean; 
                img.style.position = 'relative';
                img.onmousedown = (e : MouseEvent) => {
                    startX = e.clientX;
                    startY = e.clientY;
                    clicked = true;
                };
                img.onmousemove = (e : MouseEvent) => {
                    if (!clicked) { 
                        e.preventDefault();
                        return;
                    }
                    var diffX = e.clientX - startX;
                    var diffY = e.clientY - startY;
                    left += diffX;
                    top += diffY;
                    // adjust
                    if (left > 0) { left = 0; }
                    if (left + img.width < 200) { left = 200 - img.width; }
                    if (top > 0) { top = 0; }
                    if (top + img.height < 200) { top = 200 - img.height; }
                    img.style.left = (left + 'px');
                    img.style.top = (top + 'px');
                    startX = e.clientX;
                    startY = e.clientY;
                    e.preventDefault();
                };
                img.onmouseup = (e : MouseEvent) => {
                    clicked = false;
                    e.preventDefault();
                }
            })();
            var submitButton : any = document.querySelector('#button_submit');
            submitButton.style.visibility = 'visible';
            submitButton.onclick = () => {
                var c : any = document.createElement('canvas');
                c.width = 200;
                c.height = 200;
                console.log('top=' + top + ' left=' + left);
                var context = c.getContext("2d");
                context.drawImage(img, -left, -top, 200, 200, 0, 0, 200, 200);
                // output
                var outputFrame = document.querySelector('#output');
                var outImage = document.createElement('img');
                outImage.src = c.toDataURL("image/jpeg");
                outputFrame.appendChild(outImage);
            };
        }
    }
    // 画像読み込み
    fr.readAsDataURL(file);
}

window.onload = () => {
    var fileElement = document.querySelector('#button_file');
    (<any>fileElement).onchange = function() {
        fileChanged(this);
    };
};