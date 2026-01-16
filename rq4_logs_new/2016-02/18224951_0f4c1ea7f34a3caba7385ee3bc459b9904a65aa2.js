define(['app', 'underscore', 'authentication'], function(app, _) { 'use strict';

    return ['$scope', '$http', '$route', '$location', '$filter', '$q', function($scope, $http, $route, $location, $filter, $q) {

        var data = { title: 'agenda', content: 'loading...' };

        $scope.symbol = $route.current.params.meeting + '/' + $route.current.params.number;
        $scope.save   = save;

        load();

        return;

        //************************************************************
        //************************************************************
        //************************************************************
        //************************************************************

        function load() {

            //var data = { title: 'agenda', content: $('#content').html() };

            $.ajax({method: 'GET', url: 'https://api.cbd.int/api/v2015/tests?q={"decision":"'+$scope.symbol+'"}&fo=1', contentType: 'application/json' }).done(function( msg ) {

                msg = msg || { decision: $scope.symbol, content: 'paste here' };

                data = msg;

                $('#content').html(msg.content);
                $('#content').mousedown(function () { selectNode(this); });

                clean($('#content')[0]);

                $('element').mousedown(function () { selectNode(this); });
            });
        }

        function save() {

            selectNode(null);

            data.title = "agenda";
            data.content = $('#content').html();

            if(!data._id) {
                $.ajax({method: "POST", url: "https://api.cbd.int/api/v2015/tests", data: JSON.stringify(data), contentType: "application/json" }).done(function( msg ) {
                    data._id = msg.id;
                    alert( "Your document has been successfully created." );
                });
            } else {
                $.ajax({method: "PUT", url: "https://api.cbd.int/api/v2015/tests/"+data._id, data: JSON.stringify(data), contentType: "application/json" }).done(function( msg ) {
                    alert( "Your document has been successfully updated." );
                });
            }
        }

        function surroundSelection(tag) {

            var range = window.getSelection().getRangeAt(0);
            var range2 = range.cloneRange();

            var span = document.createElement(tag);
            var content = range.extractContents();

            span.appendChild(content);
            range.insertNode(span);

            clean($('#content')[0]);
            clean($('#content')[0]);

            $('element').mousedown(function () { selectNode(this); });

            selectNode(span);
        }

        var selectedElement = null;

        function unsurroundSelection() {

            if(!selectedElement) return alert('no selectedElement');

            $(selectedElement).contents().unwrap();

            selectedElement = null;
        }

        function editable() {

            var $div = $('div'), isEditable = $div.is('.editable');
            $div.prop('contenteditable', !isEditable).toggleClass('editable');
        }

        function selectNode(node) {

            $(selectedElement).removeClass('selected');

            selectedElement = node;

            $(selectedElement).addClass('selected');

            event.stopPropagation();
        }

        function clean(node) {

            for(var n = 0; n < node.childNodes.length; n ++) {

                var child = node.childNodes[n];

                if(child.nodeType === 8 || (child.nodeType === 3 && !/\S/.test(child.nodeValue))) {

                    node.removeChild(child);
                    n --;

                } else if(child.nodeType === 1) {

                    clean(child);

                    if(child.innerHTML=='')
                        node.removeChild(child);
                }
            }
        }

        //************************************************************
    }];
});