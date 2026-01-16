function renderhome(){
    const contentdiv = document.getElementById('content'); 


    const htmlhome = `    
    
    <header>
    <div class="headerContainer">
      <div class="logo">
        <img src="img/logo.png">
      </div>
      <nav>
        <ul>
          <li><a href="javascript:void(0)" onclick="openTab('home')">Home</a></li>
          <li><a href="javascript:void(0)" onclick="openTab('menu')">Menu</a></li>
          <li><a href="javascript:void(0)" onclick="openTab('contact')">Contact Us</a></li>
        </ul>
      </nav>
    </div>
  </header>


<div class="containerhome">
<div class="textmain"> 
  <h1>Join us and experience mouthwatering <p>dishes at affordable prices!</p></h1>
  <button onclick="openTab('menu')">Order Now</button> 
</div>
</div>
    
    
    ` ;

    contentdiv.innerHTML = htmlhome;








}


function rendermenu(){

    const contentdiv = document.getElementById("content")

    const menuhtml = `
    
    
    <header>
    <div class="headerContainer">
      <div class="logo">
      <img src="img/logo.png">
      </div>
      <nav>
        <ul>
          <li><a href="javascript:void(0)" onclick="openTab('home')">Home</a></li>
          <li><a href="javascript:void(0)" onclick="openTab('menu')">Menu</a></li>
          <li><a href="javascript:void(0)" onclick="openTab('contact')">Contact Us</a></li>
        </ul>
      </nav>
    </div>
  </header>


  <div class="project">
  <h2>Menu</h2>
  <div class="row">
  <div class="column">
    <div class="card">
      <h3>Bugger</h3>
      <p>"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce pretium accumsan leo, vel pellentesque elit malesuada vel. Morbi a justo felis. </p>
     
      <p>SSed vitae semper tortor. Aenean eget leo vel turpis blandit tincidunt ac ac turpis. Ut eu sagittis lacust</p>
     
    </div>
  </div>

  <div class="column">
    <div class="card">
      <h3>Fries</h3>
      <p>"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce pretium accumsan leo, vel pellentesque elit malesuada vel. Morbi a justo felis. </p>
      <p>SSed vitae semper tortor. Aenean eget leo vel turpis blandit tincidunt ac ac turpis. Ut eu sagittis lacust</p>
     
    </div>
  </div>






  <div class="column">
    <div class="card">
      <h3>Pizza</h3>
      <p>"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce pretium accumsan leo, vel pellentesque elit malesuada vel. Morbi a justo felis. </p>
      <p>SSed vitae semper tortor. Aenean eget leo vel turpis blandit tincidunt ac ac turpis. Ut eu sagittis lacust</p>
    
    
    </div>
  </div>
<br>
  <div class="column">
    <div class="card">
      <h3>Fries</h3>
      <p>"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce pretium accumsan leo, vel pellentesque elit malesuada vel. Morbi a justo felis. </p>
      <p>SSed vitae semper tortor. Aenean eget leo vel turpis blandit tincidunt ac ac turpis. Ut eu sagittis lacust</p>
     
    </div>
  </div>





</div>
    
    
    
    `;

    contentdiv.innerHTML = menuhtml;
}



function rendercontact(){
    const contentdiv = document.getElementById("content")
     const contacthtml = `   
     <header>
     <div class="headerContainer">
       <div class="logo">
       <img src="img/logo.png">
       </div>
       <nav>
         <ul>
           <li><a href="javascript:void(0)" onclick="openTab('home')">Home</a></li>
           <li><a href="javascript:void(0)" onclick="openTab('menu')">Menu</a></li>
           <li><a href="javascript:void(0)" onclick="openTab('contact')">Contact Us</a></li>
         </ul>
       </nav>
     </div>
   </header>


   <div class="contacthead">
   <h1> Contact Us</h1>
  
 </div>


<div class="contactcontent">

<p><ion-icon name="map"></ion-icon>
 1024 Obud Aval
 San Diego, CA 2334 </p>

 
<p><ion-icon name="call-sharp"></ion-icon> : 55545-8894</p>
 

 <h2> Send Us A Message</h2>
<div class="container">
 <form action="/action_page.php">
   <label for="fname">Name</label>
   <input type="text" id="fname" name="firstname" placeholder="Your name..">


   <label for="fname">Email</label>
   <input type="text" id="fname" name="firstname" placeholder="Email">

   

   <label for="subject">Message</label>
   <textarea id="subject" name="subject" placeholder="Write something.." style="height:200px"></textarea>

   <input type="submit" value="Submit">
 </form>


 

</div>
     
     
     
     
     `;

     contentdiv.innerHTML = contacthtml;



    }


function openTab (tabname){
    switch (tabname) {
        case 'home':
          renderhome();
          break;
          case 'menu':
            rendermenu();
            break;
          case 'contact':
            rendercontact();
            break;
          default:
            renderhome();
            break;



    }

}



















renderhome();