/// <reference path="./typings/vue/vue.d.ts"/>
import Vue = require("vue");

var demo = new Vue({
    el: '#demo',
    data: {
        title: 'todos',
        todos: [
            {
                done: true,
                content: 'Learn JavaScript'
            },
            {
                done: false,
                content: 'Learn Vue.js'
            }
        ]
    }
});