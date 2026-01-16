


import {auth, storage, db, signOut, getDoc, doc, onAuthStateChanged, getDocs, collection} from'./utils/utils.js';

const logout_btn= document.getElementById("logout_btn");
const login_link= document.getElementById("login_link");
const user_img= document.getElementById("user_img");
const events_cards_container = document.getElementById('events_cards_container');



getAllEvents()
onAuthStateChanged(auth, (user) => {
  if (user) {
    
    const uid = user.uid;
    login_link.style.display= 'none';
    user_img.style.display= 'inline-block';
    getUserInfo(uid);
    
  } else {
    window.location.href = "/auth/login/index.html";
    login_link.style.display= 'inline-block';
    user_img.style.display= 'none';
  }
});


logout_btn.addEventListener('click', ()=>{
  signout(auth);
});

function getUserInfo(uid){
  const userRef= doc(db, "users", uid);
  getDoc(userRef).then((data)=>{
    console.log("data==>",  data.id);
    console.log("data==>",  data.data());
    user_img.src= data.data()?.img;
  })
}

async function getAllEvents(){
  try{
    const querySnapshot = await getDocs(collection(db, "events"));
    events_cards_container.innerHTML= ''
    querySnapshot.forEach((doc)=>{
      console.log(`${doc.id}=>
         ${doc.data()}`);
         const event = doc.data()
         
         
         const card = ''
         console.log(event);

    });

  }catch(err){
    alert(err)
  }
  
}