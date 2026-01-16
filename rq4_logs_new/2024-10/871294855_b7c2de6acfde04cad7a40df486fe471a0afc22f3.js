const navItems = document.querySelectorAll('.nav-item');

navItems.forEach(function(item) {

    item.addEventListener('click', function() {

        navItems.forEach(function(nav) {
            nav.classList.remove('active');
        });

        item.classList.add('active');
    });
});