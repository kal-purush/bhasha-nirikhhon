/**
 * Created by smsx on 2016/11/14.
 */

function JhPage(){}
JhPage.prototype.name = "page";
JhPage.prototype.zlevel = 0;
JhPage.prototype.err = function(str){
    jhUtils.err("page name="+this.name+" info:"+str);
};
JhPage.prototype.warn = function(str){
    jhUtils.warn("page name="+this.name+" info:"+str);
};
JhPage.prototype.onCreate = function(){
    return null;
};
JhPage.prototype.onStart = function(){
    return null;
};
JhPage.prototype.onRelease = function(){
    return null;
};
