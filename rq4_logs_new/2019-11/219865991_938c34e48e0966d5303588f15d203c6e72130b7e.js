var app = new Vue({
  el: "#app",
  data:{
    textOne:" textOne ",
    textTwo:" textTwo ",
    newText:"wow",
    list:[
      "yeah"
    ]
  },//end data
  
  methods:{
    printMethod: function(p1){
       var selectH1 = document.querySelector("h1");
      selectH1.append(p1);
    },// end printMethod
    
    submitMethod: function(){
      this.list.push(this.newText);
      console.log(this.newText);
      this.newText = "";
      
    },//end submitMethod
    
    blueBoxMethod: function(){
      var selectH1Two = document.querySelector("h1");
      selectH1Two.append("You right clicked me");
    }//end blueBoxMethod
  }//end methods
  
});//end Vue