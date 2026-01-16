        window.onscroll = function() {Scrollheader()};
        var navbar = document.getElementById("header__nav-scrollbar");  
        var navbarmobile = document.getElementById("header__mobile"); 
        var sticky = navbar.offsetTop;

        function Scrollheader() {
            if((document.body.scrollTop > 80 || document.documentElement.scrollTop > 80) ) {
                navbar.style.display = "flex";   
                
            } else {
                navbar.style.display = "none";
                
            }            
        }

        

        const searchWheres = document.querySelectorAll('.js-search-where')
        const search = document.querySelector('.js-search-where-show')
        const closesw = document.querySelector ('.js-search-close')
        
        function showSearchwhere()
        {
            search.classList.add('open')
                    
        }  
        
        for (const searchWhere of searchWheres) {
            searchWhere.addEventListener('click',showSearchwhere)
        }

        //searchWhere.addEventListener('click', showSearchwhere)

        function closeSearchwhere() {
            search.classList.remove('open')
        }    
        closesw.addEventListener('click', closeSearchwhere)

        // Tìm kiếm linh hoạt
        const headerMobileShow = document.querySelector('.js-header__mobile-checked')
        const headerMobileSearch = document.querySelector('.js-header__mobile-search-show')
        const headerMobileClose = document.querySelector('.js-header__mobile-search-close')

        function showHeaderMobilesearch()
        {
            headerMobileSearch.classList.add('open')
        }

        headerMobileShow.addEventListener('click',showHeaderMobilesearch)

        function closeHeaderMobilesearchClose()
        {
            headerMobileSearch.classList.remove('open')
        }

        headerMobileClose.addEventListener('click', closeHeaderMobilesearchClose)

        // Show header 

        const clickHeaderScrollh1 = document.querySelector('.js-header__nav-scrollbar')
        const clickHeaderScroll = document.querySelector('.js-header__nav-scrollba-btn')
        const headerapp = document.querySelector('.js-scroll-header-click')
        const jsheader = document.querySelector('.js-header')
        function showHeader () {
            headerapp.classList.add('show')
            clickHeaderScroll.style.display = "none"  
            
            clickHeaderScrollh1.style.display = "none"
        }
        function removeHeader () {
            headerapp.classList.remove('show')
            clickHeaderScroll.style.display = "inline-block"  
            clickHeaderScrollh1.style.display = "flex"
        }
        clickHeaderScroll.addEventListener('click', showHeader)
        headerapp.addEventListener('click',removeHeader)
        jsheader.addEventListener('click',function(event){
            event.stopPropagation()
        })
        window.onscroll = function () {rremoveHeader()}
        function rremoveHeader() {
            headerapp.classList.remove('show')       
            if((document.body.scrollTop > 80 || document.documentElement.scrollTop > 80) ) {
                navbar.style.display = "flex";  
                 clickHeaderScroll.style.display = "inline-block" 
            } else {
                navbar.style.display = "none";              
            }             
        }
        window.addEventListener("scroll",rremoveHeader)
        
        // search

        function focuss() { 
            document.getElementById("text").focus();
        }

        clickHeaderScroll.addEventListener('click',focuss);
        //MODAL 

        const buyBtns = document.querySelectorAll('.js-sign')
        const showModal = document.querySelector('.js-modal')
        const closeModal = document.querySelector('.js-modal__sign-top-close')
        const modalContainer = document.querySelector('.js-modal-sign')
        function Showmodal(){
            showModal.classList.add('showmodal')
        }
        function closemodal(){
            showModal.classList.remove('showmodal')
        }
        for(const buyBtn of buyBtns)
        {
            buyBtn.addEventListener('click',Showmodal)
        }
        closeModal.addEventListener('click',closemodal)
        showModal.addEventListener('click',closemodal)
        modalContainer.addEventListener('click',function(event){
            event.stopPropagation()
        })

        //Page 2

        
        