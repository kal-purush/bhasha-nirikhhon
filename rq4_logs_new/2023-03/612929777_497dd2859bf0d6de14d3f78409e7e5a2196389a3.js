(function ($) {
    $(document).ready(function () {
        var controller = new ScrollMagic.Controller({addIndicators: false});
       
        function fadeUp () {
            var elements = $('[data-scroll="fadeUp"]');
            
            elements.each(function () {
                var tween = TweenMax.from(this, time, {
                    yPercent: 20,
                    autoAlpha: 0
                });
                
                var scene = new ScrollMagic.Scene({
                    triggerElement: this,
                    triggerHook: 0.9,
                })
                .setTween(tween)
                .reverse(false)
                .addTo((controller));
            })
        }

        function staggerUp () {
            var element = $('[data-scroll="staggerUp"]');
            
            element.each(function () {
                var elements = $(this).find('[data-scroll="staggerUpItem"]');
                var tween = TweenMax.staggerFrom(elements, time, {
                    yPercent: 20, 
                    autoAlpha: 0
                }, 0.15);
                
                var scene = new ScrollMagic.Scene({
                    triggerElement: this,
                    triggerHook: 0.9,
                })
                .setTween(tween)
                .reverse(false)
                .addTo((controller));
            })
        }
        
       function pin () {
           var scene = new ScrollMagic.Scene({
               triggerElement: '[data-scroll="pin"]',
               triggerHook: 1,
           })
           .setPin('[data-scroll="pin"]', {pushFollowers: false})
           .addTo(controller);
       }
        
        
        if ($(window).innerWidth() < 768) return;
        
        if ($('[data-scroll="fadeUp"]').length) fadeUp();
        if ($('[data-scroll="staggerUp"]').length) staggerUp();
       if ($('[data-scroll="pin"]').length) pin();
        
    });
})(jQuery);