if (Meteor.isClient) {
  
  Template.hello.helpers({
    image: function () {
      return  Session.get('image');
    }
  });

  Template.hello.events({
    'click button': function () {
      Meteor.call('cloudinary', function(err, response) {
      console.log(response);      
      });         
      convertImgToBase64('http://res.cloudinary.com/dhcie5spu/image/upload/v1434222137/meteor_g7nd7g.jpg'); 
      downloadAllImages( [
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222140/image_kutplc.jpg",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222124/images_piiwrc.jpg",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222137/meteor_g7nd7g.jpg",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222159/Red-Meteor-Shower_hhlmeq.jpg",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222164/perseid_dtxbr2.jpg",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222308/images_dfo1ih.png",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222322/images_2_xlwcis.jpg",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222329/images_1_vqg9dq.jpg",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222802/The_Search_is_On__Officials_Look_FOr_Meteorite_That_Fell_in_Rural_NW_Mexico_zskjlv.jpg",
       "http://res.cloudinary.com/dhcie5spu/image/upload/v1434222807/Meteorite_042513-617x416_ycixt0.jpg" 
      ] );      
    }
  });

  function convertImgToBase64(url, outputFormat){
	 var img = new Image();
	 img.crossOrigin = 'Anonymous';
	 img.onload = function(){
	   var canvas = document.createElement('CANVAS');
	   var ctx = canvas.getContext('2d');
		canvas.height = this.height;
		canvas.width = this.width;
	  	ctx.drawImage(this,0,0);
	  	var dataURL = canvas.toDataURL(outputFormat || 'image/png');
	  	Session.set('image', dataURL); 
	  	canvas = null; 
	 };
	 img.src = url;
  }
 
 
  function downloadAllImages(imgLinks){
    var zip = new JSZip();
    var deferreds = [];
    for(var i=0; i<imgLinks.length; i++){
      deferreds.push( addToZip(zip, imgLinks[i], i) );
    }
    $.when.apply(window, deferreds).done(generateZip);
  }
  function generateZip(zip){
    var content = zip.generate({type:"blob"});
    saveAs(content, "downloadImages.zip");
  }

  function addToZip(zip, imgLink, i) {
    var deferred = $.Deferred();
    JSZipUtils.getBinaryContent(imgLink, function (err, data) {
      if(err) {
        alert("Problem happened when download img: " + imgLink);
        console.log("Problem happened when download img: " + imgLink);
        deferred.resolve(zip); // ignore this error: just logging
        // deferred.reject(zip); // or we may fail the download
      } else {
        zip.file("picture"+i+".jpg", data, {binary:true});
        deferred.resolve(zip);
      }
    });
    return deferred;
  }
  
}