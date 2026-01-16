import {reverse} from "../src/main";

function t(){
  setInterval(function(){

    console.log((<any>window).require);

  },100);
}
t();

describe("A suite of basic functions", function() {
    it("reverse word",function(){
        expect("DCBA").toEqual(reverse("ABCD"));
    });
    it("test require",function(done){
      // setTimeout(function(parameter) {
      //   var golbal = <any>window;
      //   var remote = golbal.require('remote');
      //   console.log(remote);
      //   var http = remote.require('http');
      //   http.get('http://www.baidu.com',function(res){
      //     console.log("响应：" + res.statusCode);
      //     expect("DCBA").toEqual(reverse("ABCD"));
      //     done();
      //   });
      // },1000)
      t();
      //done();
    })
});