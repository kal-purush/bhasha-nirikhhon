var home_page = document.getElementById('home'),
    trending_page = document.getElementById('trending'),
    rated_page = document.getElementById('rated');
//emit click events
home_page.addEventListener('click', function(){
    window.location = '/home';
});
trending_page.addEventListener('click', function(){
    window.location = 'trending';
});
rated_page.addEventListener('click', function(){
    window.location = 'rated';
});