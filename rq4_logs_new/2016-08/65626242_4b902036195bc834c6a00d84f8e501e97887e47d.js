Vue.component('event-card', {
    template: '#event-template',

    props: {
        filterStartDate: String,
        filterPrice: String
    },

    data: function () {
        return {
            events: []
        }
    },

    filters: {
        truncate: function(string, value) {
            return string.substring(0, value) + '...';
        }

    },

    created: function () {
        this.fetchEventList();
    },

    methods: {
        fetchEventList: function () {
            $.getJSON('api/events', function (event) {
                this.events = event;
            }.bind(this));
        }
    }
});

new Vue({
    el: '#myEvents',
    data: {
        searchStartDate: '',
        searchPrice: ''
    }
});