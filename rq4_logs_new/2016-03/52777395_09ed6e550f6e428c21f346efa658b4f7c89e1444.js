var system = require('../_System.js').getInstance();

module.exports = {
    /**
     * Define a list of columns for the grid
     *
     * @var Object
     **/
    columns: [
        {data: 'id'},
        {data: 'title'},
        {data: 'filename'}
    ],

    /**
     * Renderer for the columns by the index
     *
     * @var Object
     **/
    columnDefs: [
        {
            render: function ( data, type, row ) {
                var noTags = system.stripTags(data);

                return '<a href="/admin/gallery/'+ row.id + '/edit" title="' + noTags + '">' +
                    system.ellipsis( noTags, 100 ) +
                    '</a>';
            },
            targets: 1
        }, {
            render: function( data ) {
                var img = '';
// console.log( 'data =', data );
                img = '<img width="100" height="50" src="' + data + '" ' + 'class="img-responsive img-thumbnail">';

                return img;
            },
            targets: 2
        }
    ],

    ajax: {
        url: system.getUrl( 'gallery' )
    },


};