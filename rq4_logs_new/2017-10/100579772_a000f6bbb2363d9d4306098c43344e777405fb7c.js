
Ext.define('Owl.view.window.group.Group',{
    extend: 'Ext.panel.Panel',

    requires: [
        'Owl.view.window.group.GroupController',
        'Owl.view.window.group.GroupModel'
    ],

    controller: 'window-group-group',
    viewModel: {
        type: 'window-group-group'
    },

    html: 'Hello, World!!'
});