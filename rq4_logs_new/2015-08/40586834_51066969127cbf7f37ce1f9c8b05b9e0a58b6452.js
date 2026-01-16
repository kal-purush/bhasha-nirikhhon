/* 
* Released under the MIT license by James Schroer, August 2015.
*/

/// Replace element with it's first child
var Utils={};
Utils.replaceWithChild = function(element) {
    var child = angular.element(element[0].firstChild);
    Utils.mergeAttributes(element, child);
    element.replaceWith(child);
}

/// Copy attributes from sourceElement to targetElement, merging their values if the attribute is already present
Utils.mergeAttributes = function(sourceElement, targetElement) {
    var arr = sourceElement[0].attributes;
    for(var i = 0; i < arr.length; i++) {
        var item = arr[i];
        if(!item.specified)
            continue;

        var key = item.name;
        var sourceVal = item.value;
        var targetVal = targetElement.attr(key);

        if(sourceVal === targetVal)
            continue;

        var newVal = targetVal === undefined
            ? sourceVal
            : sourceVal + ' ' + targetVal;

        targetElement.attr(key, newVal);
    }
}
    
    
    
var app=angular.module('sly',[]);
app.provider('slyCtrl',function(){
  var struct={};
  var cache={};
  var url="";
  var Suspend=false;
  var loadList={};
  var LoadListIdx=0;
  var clock=false;
  var scale=true;
  var tickCount=0;
  function tick(){
    var viewportCenterX=$(window).width()/2+$(window).scrollTop();
    var viewportCenterY=$(window).height()/2;
    var count=0;


    function check(viewportCenterX,viewportCenterY,item){
      var viewportRadius=Math.sqrt(viewportCenterX*viewportCenterX+viewportCenterY*viewportCenterY)*1.25;//The distance from a corner of the viewport to the center multiplied by 125%
      function distance(element){//Find the distance for a given element on the page from the center of the browesrs viewport
        var offset=element.offset();
        var dx = Math.max(offset.left-viewportCenterX,0,viewportCenterX-(offset.left+element.width()));
        var dy = Math.max((offset.top-element.height())-viewportCenterY,0,viewportCenterY-offset.top);
        return Math.sqrt(dx*dx+dy*dy);
      }
      var delta=distance(item.element);
      if(delta<=viewportRadius&&item.test==false){//If the element is less than viewportRadius check to see if the image needs to be loaded
        item.test=true;
        window.setTimeout(function(){//Make sure the element isn't in motion and has a chance of leaving the viewport before loading
          var delta=distance(item.element);
          if(delta<=viewportRadius&&item.test==true){
            item.callback();
            delete loadList[item.id];
          }else
            item.test=false;

        },16);
      }else{
        item.test=false;
      }
    }

    for(var i in loadList){//scan the queue of elements that maybe loaded
      var item=loadList[i];
      var element=item.element;
      if(item){
        count++;
        if(!item.test)
          check(viewportCenterX,viewportCenterY,item);
      }
    }
    if(count==0){
      window.clearInterval(clock);
      clock=false;
    }
  }
  this.Tick=function(val){//enable checking for loadable elements
    if(val&&loadList.length){
      if(!clock)
        clock=window.setInterval(tick,16);
    }else if(!val){
      window.clearInterval(clock);
      clock=false;
    }
  };
  this.ManifestUrl=function(cUrl){//Url for looking up an images dimensions 
    url=cUrl;
  };
  this.Suspend=function(val){
    Suspend=val;
  }
  this.$get=['$http','$q',function($http,$q){
    return{
      inCache:function(item){
        var hash=CryptoJS.RIPEMD160(item);
        return (hash.toString(CryptoJS.enc.Base64) in cache);
      },
      fetchAll:function(){
        var req=$http.post(url,{item:'*'});
        var defer=$q.defer();
        req.success(function(data){
          var item=data.item;
          for(var key in data)
            cache[key]=data[key];

          defer.resolve();
        });
        return defer.promise;
      },
      getMeta:function(item){ 
        var defer=$q.defer();
        if(Suspend){
          defer.resolve(undefined);
        }else{
          var hash=CryptoJS.RIPEMD160(item);
          item=hash.toString(CryptoJS.enc.Base64);
          if(item in cache){
            defer.resolve(cache[item]);
          }else{//check if other queries are happen at the same time and wait
            var req=$http.post(url,{item:item});
            req.success(function(data){
              var item=data.item;
              for(var key in data)
                cache[key]=data[key];

              defer.resolve(data[item]);
            });
            req.error(function(){
              defer.resolve(undefined);
            });
          }
        }
        return defer.promise;
      },
      delayLoad:function(el,cb){
        var t=LoadListIdx++;
        loadList[t]={element:el,test:false,callback:cb,id:t};
        el.attr('delay-load',t);
        if(clock===false)
          clock=window.setInterval(tick,66);
      },
      removeDelayLoad:function(id){
        delete loadList[id];
        if(Object.keys(loadList).length==0){
          window.clearInterval(clock);
          clock=false;
        }
      },
      scale:function(){
        return scale;
      }
    };
  }];
});
app.directive('slySrc',['$parse','slyCtrl','$http','$compile',function($parse,slyCtrl,$http,$compile){
  return{
    restrict:'A',
    priority:99,
    template:'<canvas></canvas>',
    replace:true,
    link:{
      pre:function($scope,$element,$attrs){//switch back to the img tag if the format is not able to be drawn into a canvas
        var url=$attrs['slySrc'];
        if(url.match(/svg|apng|gif$/i)){
          console.log('image format not supported by canvas. Switching back to normal img tag and disabling.');
          var newElment=$('<img/>');
          var attrs=$element[0].attributes;
          for(var i=0;i<attrs.length;i++){
            var name=attrs[i].name=='sly-src'?'ng-src':attrs[i].name; //swap the slySrc directive to the angular src 
            var val=attrs[i].value;
            newElment.attr(name,val);
          }
          $element.replaceWith(newElment);
          $compile(newElment)($scope);
        }
      },
      post:function($scope,$element,$attrs){
        var height=false;
        var width=false;
        var divisor=[];
        var dimSet=false;
        var img=new Image();
        var hasImage=false;

        $element.css('opacity',0);
        $element.css('transition','opacity 0.25s');
        function setImage(img){//draw the image into the canvas
          var ctx=$element[0].getContext('2d');
          $element[0].height=img.height;
          $element[0].width=img.width;
          ctx.drawImage(img,0,0,img.width,img.height);
          $element.css('opacity',1)
          img=undefined;
        }
        if($element.is('img')){
          console.log('canvas has been swapped back to normal image');
        }else{
          var url=$attrs['slySrc'];

          slyCtrl.getMeta(url).then(function(meta){
            if(meta!==undefined){
              height=meta.Height;
              width=meta.Width;
              if(!(meta.Divisor instanceof Array))
                divisor=[meta.Divisor];
              else
               divisor=meta.Divisor;

              dimSet=true;
              $element.attr('height',meta.Height);
              $element.attr('width',meta.Width);
            }
          });
          $element.addClass('sly-unloaded');
          var loadId=slyCtrl.delayLoad($element,function(){
            $element.removeClass('sly-unloaded');
            $element.addClass('sly-loading');
            img.onload=function(){
              $element.removeClass('sly-unloaded');
              $element.removeClass('sly-loading');
              $element.addClass('sly-loaded');
              hasImage=true;
              if(!dimSet){
                height=img.height;
                width=img.width;
                $element.attr('height',img.height);
                $element.attr('width',img.width);
              }
              setImage(img);
            };
            if(dimSet&&slyCtrl.scale()){//find the cloesest image that fits the display size of the image on screen
              var vHeight=$element.height();  
              var vWidth=$element.width();
              var scale=Math.min(Math.max(height/vHeight,1),Math.max(width/vWidth,1));

              var deltas=divisor.map(function(el){return{Delta:Math.abs(el-scale),Div:el}});
              deltas=deltas.sort(function(a,b){
                return a.Delta - b.Delta;
              });
              for(var i=0;i<deltas.length;i++) {
                if(height/deltas[i].Div>vHeight&&width/deltas[i].Div>vWidth) {
                  if(deltas[i].Div!=1){
                    url=scale!=1?url.replace(/^(.+)(\..+)$/,'$1['+deltas[i].Div+']$2'):url;
                    break;
                  }
                }
              }
              img.src=url;
            }else
              img.src=url;
          });
          $element.on('$destroy',function(){
            slyCtrl.removeDelayLoad(loadId);
          });
        }
      }
    }
  };
}]);