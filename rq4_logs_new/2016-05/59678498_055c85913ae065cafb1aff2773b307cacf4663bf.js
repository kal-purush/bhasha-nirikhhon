$(function() {
    var grid;

    grid = [
    {
        nome: "Mãe",
        imagem : "assets/image/GB40R6Jw-320.jpg",
        video : "//content.jwplatform.com/players/GB40R6Jw-SdpdX5I4.html"
    },
    {
        nome: "Pai",
        imagem : "assets/image/RrrzdtA1-320.jpg",
        video : "//content.jwplatform.com/players/RrrzdtA1-SdpdX5I4.html"
    },
    {
        nome: "Mariana",
        imagem : "assets/image/SWFcLC32-320.jpg",
        video : "//content.jwplatform.com/players/SWFcLC32-SdpdX5I4.html"
    },
    {
        nome: "Andrei",
        imagem : "assets/image/GvxF52ha-320.jpg",
        video : "//content.jwplatform.com/players/GvxF52ha-SdpdX5I4.html"
    },

    {
        nome: "Gui",
        imagem : "assets/image/F1rf43W4-320.jpg",
        video : "//content.jwplatform.com/players/F1rf43W4-SdpdX5I4.html"
    },
    {
        nome: "Vó Luzia",
        imagem : "assets/image/vo_luzia.jpg",
        video : "//content.jwplatform.com/players/hPCdADg2-SdpdX5I4.html"
    },
    {
        nome: "Shankar Nakai",
        imagem : "assets/image/cwwiB0Q1-320.jpg",
        video : "//content.jwplatform.com/players/cwwiB0Q1-SdpdX5I4.html"
    },
    {
        nome: "Luiz e Ana",
        imagem : "assets/image/ana-luiz.jpg",
        video : "//content.jwplatform.com/players/AQxd5S2T-SdpdX5I4.html"
    },
    {
        nome: "Matheus (Matemático ou Galinha)",
        imagem : "assets/image/matheus.jpg",
        video : "//content.jwplatform.com/players/U8vL0Z7t-SdpdX5I4.html"
    },
    {
        nome: "Rhalp",
        imagem : "assets/image/gNmr9Qv0-320.jpg",
        video : "//content.jwplatform.com/players/gNmr9Qv0-SdpdX5I4.html"
    },

    {
        nome: "Fer",
        imagem : "assets/image/fer.jpg",
        video : "//content.jwplatform.com/players/XZLLc5Tx-SdpdX5I4.html"
    },
    {
        nome: "Mariana AP",
        imagem : "assets/image/mariana-ap.jpg",
        video : "//content.jwplatform.com/players/tAkioP3w-SdpdX5I4.html"
    },
    {
        nome: "Patty",
        imagem : "assets/image/rkolcEpZ-320.jpg",
        video : "//content.jwplatform.com/players/rkolcEpZ-SdpdX5I4.html"
    },
    {
        nome: "Elisa",
        imagem : "assets/image/hhvBaaFF-320.jpg",
        video : "/content.jwplatform.com/players/hhvBaaFF-SdpdX5I4.html"
    },

    {
        nome: "Friends (USA)",
        imagem : "assets/image/Em4KXeTu-320.jpg",
        video : "//content.jwplatform.com/players/Em4KXeTu-SdpdX5I4.html"
    },
    {
        nome: "Friends (USA)",
        imagem : "assets/image/RgOJCmYz-320.jpg",
        video : "//content.jwplatform.com/players/RgOJCmYz-SdpdX5I4.html"
    },
    {
        nome: "Friends (USA)",
        imagem : "assets/image/friend_trem.jpg",
        video : "//content.jwplatform.com/players/c4xTed2L-SdpdX5I4.html"
    },
    {
        nome: "Friends (USA)",
        imagem : "assets/image/RPjlNf7z-320.jpg",
        video : "//content.jwplatform.com/players/RPjlNf7z-SdpdX5I4.html"
    },
    {
        nome: "Bonus para você",
        imagem : "assets/image/TO6P6yEd-320.jpg",
        video : "//content.jwplatform.com/players/TO6P6yEd-SdpdX5I4.html"
    },
    ];
    //    //    //Grab the inline template
    var html_list = document.getElementById('list_content_tpl').innerHTML;
    var list_tpl = Handlebars.compile(html_list);
    var list_render = "";


    for(index = 0; index < grid.length; index += 1 ){
        list_render += list_tpl({
            id     : index,
            nome   : grid[index].nome, 
            image  : grid[index].imagem,
            video  : grid[index].video
        });
    }

    var html_show = document.getElementById('show_content_tpl').innerHTML;
    var show_tpl = Handlebars.compile(html_show);
    var show_render = "";

    for(index = 0; index < grid.length; index += 1 ){
        show_render += show_tpl({
            id     : index,
            nome   : grid[index].nome, 
            image  : grid[index].imagem,
            video  : grid[index].video
        });
    }

    $('.gridder').append(list_render);
    $('.containers').append(show_render);

    // Call Gridder
    $('.gridder').gridderExpander({
        scroll: true,
        scrollOffset: 30,
        scrollTo: "panel",                  // panel or listitem
        animationSpeed: 400,
        animationEasing: "easeInOutExpo",
        showNav: true,                      // Show Navigation
        nextText: "Próximo",                   // Next button text
        prevText: "Anterior",               // Previous button text
        closeText: "Fechar",                 // Close button text
        onStart: function(){
            //Gridder Inititialized
        },
        onContent: function(block) {

            // var html_movie = document.getElementById('movie_content_tpl').innerHTML;
            // movie_tpl = Handlebars.compile(html_movie),
            // description = block.find('.description'),
            // id = block.find('.description').data('id');

            // show_render += movie_tpl({
            //     'id'     : id,
            //     'nome'   : grid[id].nome, 
            //     'image'  : grid[id].imagem,
            //     'video'  : grid[id].video
            // });

            // description.before(show_render);

            // $('.media_' + id).media();
        },
        onClosed: function(){
            //Gridder Closed
        }
    });

});
