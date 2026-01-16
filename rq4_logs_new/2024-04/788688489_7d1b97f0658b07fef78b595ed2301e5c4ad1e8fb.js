$(document).ready(function(){
    $(window).scroll(function(){
        // sticky navbar on scroll script
        if(this.scrollY > 20){
            $('.navbar').addClass("sticky");
        }else{
            $('.navbar').removeClass("sticky");
        }
        
        // scroll-up button show/hide script
        if(this.scrollY > 500){
            $('.scroll-up-btn').addClass("show");
        }else{
            $('.scroll-up-btn').removeClass("show");
        }
    });

    // slide-up script
    $('.scroll-up-btn').click(function(){
        $('html').animate({scrollTop: 0});
        // removing smooth scroll on slide-up button click
        $('html').css("scrollBehavior", "auto");
    });

    $('.navbar .menu li a').click(function(){
        // applying again smooth scroll on menu items click
        $('html').css("scrollBehavior", "smooth");
    });

    // toggle menu/navbar script
    $('.menu-btn').click(function(){
        $('.navbar .menu').toggleClass("active");
        $('.menu-btn i').toggleClass("active");
    });

    // typing text animation script
    var typed = new Typed(".typing", {
        strings: ["4th Year Eng student at UWaterloo", "Full Stack Developer", "Passionate Software Developer", "Team Player"],
        typeSpeed: 100,
        backSpeed: 60,
        loop: true
    });

    var typed = new Typed(".typing-2", {
        strings: ["4th Year Eng student at UWaterloo", "Full Stack Developer", "Passionate Software Developer", "Team Player"],
        typeSpeed: 100,
        backSpeed: 60,
        loop: true
    });

    // owl carousel script
    $('.carousel').owlCarousel({
        margin: 20,
        loop: true,
        autoplayTimeOut: 2000,
        autoplayHoverPause: true,
        responsive: {
            0:{
                items: 1,
                nav: false
            },
            600:{
                items: 2,
                nav: false
            },
            1000:{
                items: 3,
                nav: false
            }
        }
    });
});



// Skill Percentile

{/* <div class="bars">
                        <div class="info">
                            <p class="skilllanguage">Python</p>
                            <!-- <span>80%</span> -->
                        </div>
                        <div class="line python"></div>
                    </div>
                    <div class="bars">
                        <div class="info">
                            <span>JavaScript, Node.js, </span>
                            <span>70%</span>
                        </div>
                        <div class="line js"></div>
                    </div>

                    <div class="bars">
                        <div class="info">
                            <span>Databases (SQL, MongoDB, Postgres, mysql)</span>
                            <span>80%</span>
                        </div>
                        <div class="line db"></div>
                    </div>
                    <div class="bars">
                        <div class="info">
                            <span>Datastructures and Algorithms</span>
                            <span>70%</span>
                        </div>
                        <div class="line dt"></div>
                    </div>
                    <div class="bars">
                        <div class="info">
                            <span>HTML/CSS</span>
                            <span>100%</span>
                        </div>
                        <div class="line html"></div>
                    </div>
                    <div class="bars">
                        <div class="info">
                            <span>Machine Learning/AI</span>
                            <span>40%</span>
                        </div>
                        <div class="line ai"></div>
                    </div> */}