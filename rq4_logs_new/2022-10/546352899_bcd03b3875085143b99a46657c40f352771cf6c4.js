searchFormBtn.addEventListener('click',()=>{
  location.hash='#search='+ searchFormInput.value;
});

trendingBtn.addEventListener('click',()=>{
  location.hash='#trends=';
});

arrowBtn.addEventListener('click',()=>{
  //location.hash='#home';
  location.hash = window.history.back();
});


window.addEventListener('DOMContentLoaded',navigator,false);
window.addEventListener('hashchange',navigator,false);

function navigator(){
  console.log({location});// donde estamos ubicados

  if(location.hash.startsWith('#trends=')){
    trendsPage();
  }else if(location.hash.startsWith('#search=')){
    searchPage();
  }else if(location.hash.startsWith('#movie=')){
    moviesPage();
  }else if(location.hash.startsWith('#category=')){
    categoriesPage();
  }else{
    homePage();
  }
  document.documentElement.scrollTop=0;// para que no aparezca el scroll abajo
  document.body.scrollTop=0; // para que no aparezca el scroll abajo

  //location.hash
}

function homePage(){
  console.log('Home!!');

  headerSection.classList.remove('header-container--long');
  headerSection.style.background = '';
  arrowBtn.classList.add('inactive');
  arrowBtn.classList.remove('header-arrow--white');
  headerTitle.classList.remove('inactive');
  headerCategoryTitle.classList.add('inactive');
  searchForm.classList.remove('inactive');

  trendingPreviewSection.classList.remove('inactive');
  categoriesPreviewSection.classList.remove('inactive');
  genericSection.classList.add('inactive');
  movieDetailSection.classList.add('inactive');

  getTrendingMoviesPreview();
  getCategegoriesPreview();
}

function trendsPage(){
  console.log('Trends!!');

  headerSection.classList.remove('header-container--long');
  headerSection.style.background = '';
  arrowBtn.classList.remove('inactive');
  arrowBtn.classList.remove('header-arrow--white');
  headerTitle.classList.add('inactive');
  headerCategoryTitle.classList.remove('inactive');
  searchForm.classList.add('inactive');

  trendingPreviewSection.classList.add('inactive');
  categoriesPreviewSection.classList.add('inactive');
  genericSection.classList.remove('inactive');
  movieDetailSection.classList.add('inactive');

  headerCategoryTitle.innerHTML= 'Tendencias';
  getTrendingMovies();
}

function searchPage(){
  console.log('Search!');

  headerSection.classList.remove('header-container--long');
  headerSection.style.background = '';
  arrowBtn.classList.remove('inactive');
  arrowBtn.classList.remove('header-arrow--white');
  headerTitle.classList.add('inactive');
  headerCategoryTitle.classList.add('inactive');
  searchForm.classList.remove('inactive');

  trendingPreviewSection.classList.add('inactive');
  categoriesPreviewSection.classList.add('inactive');
  genericSection.classList.remove('inactive');
  movieDetailSection.classList.add('inactive');

  const [_,busqueda] =location.hash.split('=');// ['#search','busqueda']
  getMoviesBySearch(busqueda);
}

function moviesPage(){
  console.log('Movies!!');

  headerSection.classList.add('header-container--long');
  arrowBtn.classList.remove('inactive');
  arrowBtn.classList.add('header-arrow--white');
  headerTitle.classList.add('inactive');
  headerCategoryTitle.classList.add('inactive');
  searchForm.classList.add('inactive');

  trendingPreviewSection.classList.add('inactive');
  categoriesPreviewSection.classList.add('inactive');
  genericSection.classList.add('inactive');
  movieDetailSection.classList.remove('inactive');

  const [_,busqueda] =location.hash.split('=');// ['movie','id']

  getMovieById(busqueda);
}

function categoriesPage(){
  console.log('Categories!!')

  headerSection.classList.remove('header-container--long');
  headerSection.style.background = '';
  arrowBtn.classList.remove('inactive');
  arrowBtn.classList.remove('header-arrow--white');
  headerTitle.classList.add('inactive');
  headerCategoryTitle.classList.remove('inactive');
  searchForm.classList.add('inactive');

  trendingPreviewSection.classList.add('inactive');
  categoriesPreviewSection.classList.add('inactive');
  genericSection.classList.remove('inactive');
  movieDetailSection.classList.add('inactive');

  const [_,categoryInfo] =location.hash.split('=');// ['#category','id-genero']
  const [categoryId,categoryName] = categoryInfo.split('-');

  headerCategoryTitle.innerHTML=decodeURIComponent(categoryName); // decodeUriComponent para que no aparezca mal escrito el name

  // get movies by category
  getMoviesByCategory(categoryId);
}