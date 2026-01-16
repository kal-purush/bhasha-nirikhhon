// app.js
define([
    "constant",
    "orderServer",
    'text!page/member/index.html',
    'less!page/member/index'
], function(constant, orderServer, tpl) {
	return {
        template: tpl,
        components: {},
         data() {
            return {
                title: '用户管理'
            }
        },
        mounted: function () {},
        created: function() {},
        methods: {},
    }
});
