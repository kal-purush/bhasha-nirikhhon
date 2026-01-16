define(["jquery"], function($) {
    'use strict';


    $(function() {
        var $body = $('body');

        (function _loopColor() {
            $body.addClass('background-color--green');

            setTimeout(function() {
                $body.removeClass('background-color--green');
                $body.addClass('background-color--red');

                setTimeout(function() {
                    $body.removeClass('background-color--red');
                    $body.addClass('background-color--green');

                    _loopColor();
                }, 4000);
            }, 4000);

        }(0));

        var $g1 = $('.g1');
        var $g2 = $('.g2');
        var $g3 = $('.g3');
        var $g4 = $('.g4');
        var $g5 = $('.g5');
        var $g6 = $('.g6');

        (function _loopGear() {
            $g1.addClass('up');
            $g2.removeClass('down');

            setTimeout(function() {
                $g2.addClass('up');
                $g3.removeClass('down');

                setTimeout(function() {
                    $g3.addClass('up');
                    $g4.removeClass('down');

                    setTimeout(function() {
                        $g4.addClass('up');
                        $g5.removeClass('down');

                        setTimeout(function() {
                            $g5.addClass('up');
                            $g6.removeClass('down');

                            setTimeout(function() {
                                $g6.addClass('down');
                                $g5.removeClass('up');


                                setTimeout(function() {
                                    $g5.addClass('down');
                                    $g4.removeClass('up');

                                    setTimeout(function() {
                                        $g4.addClass('down');
                                        $g3.removeClass('up');

                                        setTimeout(function() {
                                            $g3.addClass('down');
                                            $g2.removeClass('up');

                                            setTimeout(function() {
                                                $g2.addClass('down');
                                                $g1.removeClass('up');

                                                setTimeout(function() {
                                                    _loopGear();
                                                }, 1000);
                                            }, 300);
                                        }, 300);
                                    }, 300);
                                }, 300);
                            }, 600);
                        }, 2200);
                    }, 2000);
                }, 1800);
            }, 800);
        }(500));
    });
});