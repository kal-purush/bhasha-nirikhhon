//var folder = "http://demo.smart2pay.com/HppMerchant/_marketing/S2P_logo/";
/*
var folder = 'img/';

$.ajax({
    url : folder,
    crossDomain: false,
    success: function (data) {
        $(data).find("a").attr("href", function (i, val) {
            if( val.match(/\.(jpe?g|png|gif)$/) ) { 
                $("body").append( "<img src='"+ folder + val +"'>" );
            } 
        });
    }
});
*/

let _DATA = [
    [
        'Logo',
        {
            filters: ['name', 'width', 'height', 'dpi', 'format'],
            images: [
                {
                    name: 's2p_logo_purple_bkg.eps',
                    width: -1,
                    height: -1,
                    dpi: -1
                },
                {
                    name: 's2p_logo_purple_bkg_1000x260_300dpi.png',
                    width: 1000,
                    height: 260,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_150x39_96dpi.gif',
                    width: 150,
                    height: 39,
                    dpi: 96
                },
                {
                    name: 's2p_logo_purple_bkg_240x240_300dpi.png',
                    width: 240,
                    height: 240,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_250x65_300dpi.png',
                    width: 250,
                    height: 65,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_255x255_300dpi.png',
                    width: 255,
                    height: 255,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_255x255_300dpi_v2.png',
                    width: 255,
                    height: 255,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_255x66_300dpi.png',
                    width: 255,
                    height: 66,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_300x100_300dpi.png',
                    width: 300,
                    height: 100,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_5000x1300_300dpi.png',
                    width: 5000,
                    height: 1300,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_500x130_300dpi.png',
                    width: 500,
                    height: 130,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_512x512_300dpi.png',
                    width: 512,
                    height: 512,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_bkg_reverted_150x39_96dpi.gif',
                    width: 150,
                    height: 39,
                    dpi: 96
                },
                {
                    name: 's2p_logo_purple_inverted_1000x258_300dpi.png',
                    width: 1000,
                    height: 258,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_inverted_240x240_300dpi_transparent.png',
                    width: 240,
                    height: 240,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_inverted_250x65_300dpi.png',
                    width: 250,
                    height: 65,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_inverted_255x255_300dpi_transparent.png',
                    width: 255,
                    height: 255,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_inverted_5000x1290_300dpi.png',
                    width: 5000,
                    height: 1290,
                    dpi: 300
                },
                {
                    name: 's2p_logo_purple_inverted_500x129_300dpi.png',
                    width: 500,
                    height: 129,
                    dpi: 300
                },
                {
                    name: 's2p_logo_transparent_bkg_1000x168_300dpi.png',
                    width: 1000,
                    height: 168,
                    dpi: 300
                },
                {
                    name: 's2p_logo_transparent_bkg_150x26_96dpi.png',
                    width: 150,
                    height: 26,
                    dpi: 300
                },
                {
                    name: 's2p_logo_transparent_bkg_250x42_300dpi.png',
                    width: 250,
                    height: 42,
                    dpi: 300
                },
                {
                    name: 's2p_logo_transparent_bkg_255x255_300dpi.png',
                    width: 255,
                    height: 255,
                    dpi: 300
                },
                {
                    name: 's2p_logo_transparent_bkg_255x43_300dpi.png',
                    width: 255,
                    height: 43,
                    dpi: 300
                },
                {
                    name: 's2p_logo_transparent_bkg_5000x840_300dpi.png',
                    width: 5000,
                    height: 840,
                    dpi: 300
                },
                {
                    name: 's2p_logo_transparent_bkg_500x500_300dpi.png',
                    width: 500,
                    height: 500,
                    dpi: 300
                },
                {
                    name: 's2p_logo_transparent_bkg_500x84_300dpi.png',
                    width: 500,
                    height: 84,
                    dpi: 300
                },
                {
                    name: 's2p_logo_white_bkg_150x26_96dpi.gif',
                    width: 150,
                    height: 26,
                    dpi: 96
                },
                {
                    name: 's2p_logo_white_bkg_255x255_300dpi.png',
                    width: 255,
                    height: 255,
                    dpi: 96
                },
                {
                    name: 's2p_logo_white_bkg_255x255_300dpi.png',
                    width: 255,
                    height: 255,
                    dpi: 96
                },
                {
                    name: 's2p_logo_white_bkg_500x500_300dpi.png',
                    width: 500,
                    height: 500,
                    dpi: 300
                },
                {
                    name: 's2p_sqaure_logo_240x240.png',
                    width: 240,
                    height: 240,
                    dpi: 300
                },
                {
                    name: 's2p_square_logo_255x255.png',
                    width: 255,
                    height: 255,
                    dpi: 300
                },
                {
                    name: 's2p_square_logo_16x16.png',
                    width: 16,
                    height: 16,
                    dpi: 300
                },
                {
                    name: 's2p_square_logo_32x32.png',
                    width: 32,
                    height: 32,
                    dpi: 300
                },
                {
                    name: 's2p_square_logo_460x460.png',
                    width: 460,
                    height: 460,
                    dpi: 300
                },
                {
                    name: 's2p_square_logo_150x150.png',
                    width: 150,
                    height: 150,
                    dpi: 300
                },
                {
                    name: 's2p_square_logo_192x192.png',
                    width: 192,
                    height: 192,
                    dpi: 300
                },
                {
                    name: 's2p_square_logo_512x512.png',
                    width: 512,
                    height: 512,
                    dpi: 300
                },
                {
                    name: 's2p_square_logo_57x57.png',
                    width: 57,
                    height: 57,
                    dpi: 300
                },
                {
                    name: 's2p-logo-purple-bkg.svg',
                    width: -1,
                    height: -1,
                    dpi: -1
                },
                {
                    name: 's2p-logo-white-bkg.svg',
                    width: -1,
                    height: -1,
                    dpi: -1
                },
                {
                    name: 's2p_square_logo_icon.ico',
                    width: 32,
                    height: 32,
                    dpi: 72
                },
                
            ]
        }
    ],
    [
        'Prints',
        {
            filters: ['width', 'height', 'dpi', 'format'],
        }
    ],
    [
        'GP Logos',
        {
            filters: ['id', 'name']
        }
    ]
];