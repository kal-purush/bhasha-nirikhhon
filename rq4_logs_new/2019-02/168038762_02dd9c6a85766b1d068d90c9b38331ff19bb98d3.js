document.getElementById('addPost').addEventListener('click', () =>{

    document.getElementById('addpost_container').style.display ='block';
    document.getElementById('index_page').style.display='none';
    document.getElementById('search_container').style.display ='none';
    document.getElementById('profile_container').style.display ='none';
    document.getElementById('recipes_container').style.display ='none';

})


document.getElementById('index').addEventListener('click', () =>{

    document.getElementById('addpost_container').style.display ='none';
    document.getElementById('index_page').style.display='block';
    document.getElementById('search_container').style.display ='none';
    document.getElementById('profile_container').style.display ='none';
    document.getElementById('recipes_container').style.display ='none';

})

document.getElementById('search').addEventListener('click', () =>{

    document.getElementById('addpost_container').style.display ='none';
    document.getElementById('index_page').style.display='none';
    document.getElementById('search_container').style.display ='block';
    document.getElementById('profile_container').style.display ='none';
    document.getElementById('recipes_container').style.display ='none';

})

document.getElementById('recipes').addEventListener('click', () =>{

    document.getElementById('addpost_container').style.display ='none';
    document.getElementById('index_page').style.display='none';
    document.getElementById('search_container').style.display ='block';
    document.getElementById('profile_container').style.display ='none';
    document.getElementById('recipes_container').style.display ='block';

})

// active buttons footer

let btns = document.getElementsByClassName('btn-act');
for (let i = 0; i < btns.length; i++) {
  btns[i].addEventListener('click', function() {
  let current = document.getElementsByClassName('active');
  current[0].className = current[0].className.replace(' active', '');
  this.className += ' active';
  });
}