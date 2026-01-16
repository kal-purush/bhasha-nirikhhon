Vue.component('modal', {
    template: document.querySelector('#modal'),
    props: {name: String},
    data: function () {
        return {
            showModal: false
        }
    },
    mounted() {
        this.$root.$on('toggleModal', ev => {
            if(ev === this.name){
                this.toggleModal();
            }
        })
    },
    methods: {
        toggleModal: function () {
            this.showModal = !this.showModal;
        }
    }

});