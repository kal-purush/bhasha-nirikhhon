var divctl;
var state = {
    change (newstate){
        var dadivs = document.getElementsByClassName("staged");
        for(x of dadivs){
            x.classList.remove('staged');
            x.classList.add('hide');
        }
            
        switch (newstate){
        case "home":
            document.body.style.backgroundImage="url('img/cello.jpg')";
            document.body.style.backgroundRepeat="no-repeat";
            document.body.style.backgroundSize="cover";
            document.getElementById("start").classList.remove('hide');
            document.getElementById("start").classList.add('staged');           
            break;
        case "about":
            document.body.style.backgroundImage="url('img/shop.jpg')";
            document.body.style.backgroundRepeat="no-repeat";
            document.body.style.backgroundSize="cover";
            document.getElementById("about").classList.remove('hide')
            document.getElementById("about").classList.add("staged");
            break;
        case "projects":
            document.body.style.background="none";
            document.body.style.backgroundColor="#000000";
            //document.getElementById("contact").classList.remove('hide');
            document.getElementById("projects").classList.remove('hide');
            document.getElementById("projects").classList.add("staged");
           // document.getElementById("contact").classList.add('staged');
            break;
        }
    }
}