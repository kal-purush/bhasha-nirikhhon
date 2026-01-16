( function(){

    $( function() {

        $.each( $( '.services__faq-list' ), function() {

            new Accordion ( $( this ) );

        } );

    });

    var Accordion = function ( obj ) {

        //private properties
        var _self = this,
            _obj = obj,
            _accordionItem = _obj.find( '.services__faq-item' ),
            _accordionSub = _obj.find( '.services__faq-list-sub' ),
            _accordionBtn = _accordionItem.find( '.services__faq-item-head' ),
            _body = $('body');

        //private methods
        var _onEvents = function () {

                _accordionBtn.on({
                    click: function(){

                        _slideAccordion( $(this) );

                        return false

                    }
                });
                _obj.on( {
                    click: function( event ) {

                        event = event || window.event;

                        if ( event.stopPropagation ) {
                            event.stopPropagation();
                        } else {
                            event.cancelBubble = true;
                        }

                    }
                } );
                _body.on( {
                    click: function() {

                        _accordionItem.removeClass( 'active' );
                        _accordionSub.slideUp( 300 );

                    }
                } );


            },
            _slideAccordion = function ( elem ) {

                var curItem = elem,
                    curParent = curItem.parent(),
                    curMenu = curParent.find( '.services__faq-list-sub' );

                if ( curParent.hasClass( 'active' ) ) {

                    curParent.removeClass( 'active' );
                    curMenu.slideUp( 300 );

                }else{

                    _accordionItem.removeClass( 'active' );
                    _accordionSub.slideUp( 300 );
                    curParent.addClass( 'active' );
                    curMenu.slideDown( 300 );

                }

                return false
            },
            _init = function () {
                _onEvents();
            };

        _init();
    };

} )();
