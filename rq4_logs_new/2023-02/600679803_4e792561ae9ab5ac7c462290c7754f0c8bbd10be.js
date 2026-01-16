//Add active class to current menu item

let navContainer = document.getElementById("nav_list");
let navItems = navContainer.getElementsByClassName("item_link");

for(let i = 0; i < navItems.length; i++) {
  navItems[i].addEventListener("click", function(){
    let curItem = document.getElementsByClassName("active");
    curItem[0].className = curItem[0].className.replace(" active", "");
    this.className += " active";
    menu.classList.remove('menu_open');
    logo.classList.remove('menu_open');
    blackout.style.display = 'none';
    document.body.classList.remove('popup');
  });
}

//add burger menu
let burger_menu = document.getElementById("burger_menu");
let menu = document.getElementById("header_navigation");
let logo = document.getElementById("header_logo");
let blackout = document.querySelector('.blackout');
burger_menu.addEventListener("click", function(){
  menu.classList.toggle('menu_open');
  logo.classList.toggle('menu_open');

  if(menu.classList.contains('menu_open')) {
    blackout.style.display = 'block';
    document.body.classList.toggle('popup');
  } else {
    blackout.style.display = 'none';
    document.body.classList.remove('popup');
  }
});

blackout.addEventListener('click', function() {
  if(menu.classList.contains('menu_open')) {
    menu.classList.remove('menu_open');
    logo.classList.remove('menu_open');
    blackout.style.display = 'none';
    document.body.classList.remove('popup');
  }
});

//add slider functionality
const pets = [
  {
    "name": "Jennifer",
    "img": "../../assets/images/jennifer.png",
    "type": "Dog",
    "breed": "Labrador",
    "description": "Jennifer is a sweet 2 months old Labrador that is patiently waiting to find a new forever home. This girl really enjoys being able to go outside to run and play, but won't hesitate to play up a storm in the house if she has all of her favorite toys.",
    "age": "2 months",
    "inoculations": ["none"],
    "diseases": ["none"],
    "parasites": ["none"]
  },
  {
    "name": "Sophia",
    "img": "../../assets/images/sophia.png",
    "type": "Dog",
    "breed": "Shih tzu",
    "description": "Sophia here and I'm looking for my forever home to live out the best years of my life. I am full of energy. Everyday I'm learning new things, like how to walk on a leash, go potty outside, bark and play with toys and I still need some practice.",
    "age": "1 month",
    "inoculations": ["parvovirus"],
    "diseases": ["none"],
    "parasites": ["none"]
  },
  {
    "name": "Woody",
    "img": "../../assets/images/woody.png",
    "type": "Dog",
    "breed": "Golden Retriever",
    "description": "Woody is a handsome 3 1/2 year old boy. Woody does know basic commands and is a smart pup. Since he is on the stronger side, he will learn a lot from your training. Woody will be happier when he finds a new family that can spend a lot of time with him.",
    "age": "3 years 6 months",
    "inoculations": ["adenovirus", "distemper"],
    "diseases": ["right back leg mobility reduced"],
    "parasites": ["none"]
  },
  {
    "name": "Scarlett",
    "img": "../../assets/images/scarlett.png",
    "type": "Dog",
    "breed": "Jack Russell Terrier",
    "description": "Scarlett is a happy, playful girl who will make you laugh and smile. She forms a bond quickly and will make a loyal companion and a wonderful family dog or a good companion for a single individual too since she likes to hang out and be with her human.",
    "age": "3 months",
    "inoculations": ["parainfluenza"],
    "diseases": ["none"],
    "parasites": ["none"]
  },
  {
    "name": "Katrine",
    "img": "../../assets/images/katrine.png",
    "type": "Cat",
    "breed": "British Shorthair",
    "description": "Katrine is a beautiful girl. She is as soft as the finest velvet with a thick lush fur. Will love you until the last breath she takes as long as you are the one. She is picky about her affection. She loves cuddles and to stretch into your hands for a deeper relaxations.",
    "age": "6 months",
    "inoculations": ["panleukopenia"],
    "diseases": ["none"],
    "parasites": ["none"]
  },
  {
    "name": "Timmy",
    "img": "../../assets/images/timmy.png",
    "type": "Cat",
    "breed": "British Shorthair",
    "description": "Timmy is an adorable grey british shorthair male. He loves to play and snuggle. He is neutered and up to date on age appropriate vaccinations. He can be chatty and enjoys being held. Timmy has a lot to say and wants a person to share his thoughts with.",
    "age": "2 years 3 months",
    "inoculations": ["calicivirus", "viral rhinotracheitis"],
    "diseases": ["kidney stones"],
    "parasites": ["none"]
  },
  {
    "name": "Freddie",
    "img": "../../assets/images/freddie.png",
    "type": "Cat",
    "breed": "British Shorthair",
    "description": "Freddie is a little shy at first, but very sweet when he warms up. He likes playing with shoe strings and bottle caps. He is quick to learn the rhythms of his human’s daily life. Freddie has bounced around a lot in his life, and is looking to find his forever home.",
    "age": "2 months",
    "inoculations": ["rabies"],
    "diseases": ["none"],
    "parasites": ["none"]
  },
  {
    "name": "Charly",
    "img": "../../assets/images/charly.png",
    "type": "Dog",
    "breed": "Jack Russell Terrier",
    "description": "This cute boy, Charly, is three years old and he likes adults and kids. He isn’t fond of many other dogs, so he might do best in a single dog home. Charly has lots of energy, and loves to run and play. We think a fenced yard would make him very happy.",
    "age": "8 years",
    "inoculations": ["bordetella bronchiseptica", "leptospirosis"],
    "diseases": ["deafness", "blindness"],
    "parasites": ["lice", "fleas"]
  }
];


let wrapper_track = document.querySelector('#wrapper_track');
for(let i = 0; i < pets.length; i++) {
  let card = document.createElement('div');
  card.className = 'card';
  let card_img = document.createElement('div');
  card_img.className = 'card_img';
  let card_title = document.createElement('div');
  card_title.className = 'card_title';
  let card_link = document.createElement('button');
  card_link.className = 'card_link';

  card_img.style.background = "url(" + `${pets[i].img}` + ")";
  card_title.textContent = pets[i].name;
  card_link.innerHTML += "<a>Learn more</a>";

  card.appendChild(card_img);
  card.appendChild(card_title);
  card.appendChild(card_link);

  wrapper_track.appendChild(card);

  card.addEventListener("click",function(){
    let card_popup = document.createElement('div');
    card_popup.className = 'card_popup';
    card_popup.style.display = "flex";
    document.body.classList.toggle('popup');

    let close_button = document.createElement('button');
    close_button.className = 'close_button';
    close_button.innerHTML = "<span>&#10006</span>";

    let popup_window = document.createElement('div');
    popup_window.className = 'popup_window';

    let popup_content = document.createElement('div');
    popup_content.className = 'popup_content';

    let popup_img = document.createElement('div');
    popup_img.className = 'popup_img';
    popup_img.style.background = "url(" + `${pets[i].img}` + ")";

    let popup_text = document.createElement('div');
    popup_text.className = 'popup_text';

    let text_title = document.createElement('h3');
    text_title.className = 'text_title';
    text_title.textContent = pets[i].name;

    let text_subTitle = document.createElement('h4');
    text_subTitle.className = 'text_subTitle';
    text_subTitle.textContent = pets[i].type + ' - ' + pets[i].breed;

    let text_descr = document.createElement('h5');
    text_descr.className = 'text_descr';
    text_descr.textContent = pets[i].description;

    let text_list = document.createElement('ul');
    text_list.className = 'text_list';

    let pet_age = document.createElement('li');
    pet_age.className = 'pet_age';
    pet_age.innerHTML = "<span>Age:&nbsp</span>" + pets[i].age;

    let pet_inoculations = document.createElement('li');
    pet_inoculations.className = 'pet_inoculations';
    pet_inoculations.innerHTML = "<span>Inoculations:&nbsp</span> " + pets[i].inoculations;

    let pet_diseases = document.createElement('li');
    pet_diseases.className = 'pet_diseases';
    pet_diseases.innerHTML = "<span>Diseases:&nbsp</span> " + pets[i].diseases;

    let pet_parasites = document.createElement('li');
    pet_parasites.className = 'pet_parasites';
    pet_parasites.innerHTML = "<span>Parasites:&nbsp</span> " + pets[i].parasites;

    popup_window.appendChild(close_button);
    popup_content.appendChild(popup_img);
    popup_text.appendChild(text_title);
    popup_text.appendChild(text_subTitle);
    popup_text.appendChild(text_descr);
    text_list.appendChild(pet_age);
    text_list.appendChild(pet_inoculations);
    text_list.appendChild(pet_diseases);
    text_list.appendChild(pet_parasites);
    popup_text.appendChild(text_list);

    popup_content.appendChild(popup_text);
    popup_window.appendChild(popup_content);

    card_popup.appendChild(popup_window);
    card.appendChild(card_popup);

    close_button.onclick = function(event) {
      event.stopPropagation();
    };

    card_popup.onclick = function(event) {
      event.stopPropagation();
    };

    popup_window.onclick = function(event) {
      event.stopPropagation();
    };
    
    close_button.addEventListener("click", function() {
        card_popup.remove();
        document.body.classList.remove('popup');
    });

    card_popup.addEventListener("click", function() {
      card_popup.remove();
      document.body.classList.remove('popup');
  });
  });
}



let wrap = [...document.querySelectorAll('.wrapper_track')];
let slides = [...document.querySelectorAll('.card')];
let next_button = document.getElementById('next_arrow');
let prew_button = document.getElementById('prew_arrow');

let curInd = 0;

let curSlides = (ind) => {
  if(ind > slides.length) {
    curInd = 1;
  }
  if(ind < 0) {
    curInd = slides.length - 3;
  }
}
let count = 3;
next_button.addEventListener("click", function(){
  for(let j = 0; j < count; j++) {
    wrap.forEach(i => i.append(slides[j]));
  }
  count+=3;
  if(count >= 12) {
    count = 3;
  }
  (document.querySelector('.wrapper_track').lastChild).remove();
});

prew_button.addEventListener("click",function() {
  for(let j = 0; j < count; j++) {
      wrap.forEach(i => i.append(slides[j]));
  }
  count+=3;
  if(count >= 12) {
    count = 3;
  }
  (document.querySelector('.wrapper_track').lastChild).remove();
});