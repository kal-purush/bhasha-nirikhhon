/*Name this external file gallery.js*/

function upDate(previewPic){
    
    document.getElementById('image').style.background="url('"+previewPic.src+"')";
    document.getElementById('image').innerHTML=previewPic.alt;

}

function unDo(){
    
    var pic = "Hover over an image below to display here."

    document.getElementById('image').innerHTML=pic;
    document.getElementById('image').style.background='#8e68ff';
    document.getElementById('image').style.backgroundImage="url('')";
    
}