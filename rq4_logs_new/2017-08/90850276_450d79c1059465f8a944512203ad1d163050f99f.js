/**
 * Created by dachen on 2017/7/13.
 */
(function(){
    "use strict";
    var myCanvas = document.getElementById('myCanvas');

    var ctx = myCanvas.getContext('2d');

    ctx.strokeStyle = 'red';
    ctx.beginPath();
    ctx.lineTo(100,200);
    ctx.lineTo(100,40);
    ctx.stroke();
    //isContextMenu 是否取消默认右键菜单，默认 false 取消
    var Canvas = function(id, isContextMenu){
        if(!id){
            return;
        }
        this._element = document.getElementById(id).getContext('2d');
        if(!isContextMenu){
            document.getElementById(id).oncontextmenu = function(){
                return false;
            };
        }
    };

    Canvas.prototype = {

      //创建新的画布，id,setting 为样式对象，key:value 形式，同css 书写
      create: function(id, setting){
          var element = document.createElement('canvas');
          for(var key in setting){
              if(setting.hasOwnProperty(key)){
                  element.style[key] = setting[key];
              }
          }
          element.setAttribute('width',setting.width? setting.width: 100);
          element.setAttribute('height', setting.height? setting.height: 100);
          element.id = id;
          document.getElementsByTagName('body')[0].appendChild(element);
          this._element = element.getContext('2d');
          element.oncontextmenu = function(){
              return false;
          };
          return this;
      },
        //设置画布, params 为样式对象， key:value 形式
      initStyle: function(params){
          for(var key in params){
              if(params.hasOwnProperty(key)){
                  this._element[key] = params[key];
              }
          }
      },
        //绘画路径， 传入二维数组，对应x, y坐标
      canvas: function(lineArr){
          for(var i=0 ; i<lineArr.length; i++){
              this._element.lineTo(lineArr[i][0],lineArr[i][1]);
          }
          this._element.stroke();
          return this;
      },
        //开始绘制
      beginPath: function(){
          this._element.beginPath();
          return this;
      },
      //擦除
      clear: function(x, y, n){
          this._element.clearRect(x, y, n, n);
      }
    };

    // var canvas1 = new Canvas();
    // canvas1.create('canvas1',{
    //     width:300,
    //     height:300,
    //     border:'1px solid #c8c8c8',
    //     position:'fixed',
    //     left:'100px',
    //     top:'100px'
    // });
    // canvas1.canvas([[10,20],[10,50]]);
    var isClear = false; // 擦除开关
    var myCanvasDom = document.getElementById('myCanvas'),
        canvas = new Canvas('myCanvas');

    function mousemoveFn(e){
        canvas.canvas([[e.clientX, e.clientY]]);
    }

    //鼠标按下事件
    myCanvasDom.addEventListener('mousedown', function(e){
        //如果是鼠标右键， 取消擦除移动事件监听
        if(e.button === 2){
            e.preventDefault();
            e.stopPropagation();
            isClear = false;
            clear.removeEventListener('mousemove',clearFn);
        }
        //如果点击了擦除，添加擦除事件监听
        else if(isClear){
            myCanvasDom.addEventListener('mousemove',clearFn );
        }
        else{
            canvas.beginPath();
            myCanvasDom.addEventListener('mousemove', mousemoveFn, false);
        }
    },false);
    //鼠标抬起事件-- 取消鼠标移动监听
    myCanvas.addEventListener('mouseup', function(){
        myCanvasDom.removeEventListener('mousemove',mousemoveFn);
        myCanvasDom.removeEventListener('mousemove',clearFn);
    });
    //鼠标滚轮事件--改变线条宽度
    myCanvas.addEventListener('mousewheel',function(e){
        var lineWidthOld = canvas._element.lineWidth;
        if(e.deltaY >0){
            lineWidthOld++;
            canvas.initStyle({lineWidth: lineWidthOld});
        }else{
            lineWidthOld--;
            canvas.initStyle({lineWidth: lineWidthOld});
        }
    }, false);

    //擦除
    var clear = document.getElementById('clearBtn');

    function clearFn(e){
        canvas.clear(e.clientX, e.clientY, 10);
    }
    clear.addEventListener('click', function(){
        isClear = true;
    });

})();