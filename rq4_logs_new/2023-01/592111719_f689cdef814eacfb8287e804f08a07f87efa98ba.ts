import { Options, Vue } from 'vue-class-component'

@Options({
    props: {
        chat: {
            type: Object,
            required: false
        },
        messages: {
            type: Array,
            required: true
        }
    },

    data: function () {
        return {
            value: ''
        }
    },

    computed: {
        currentMessages: function () {
            if (this.chat)
                return this.messages[this.chat.id];
            else 
                return [];
        }
    },

    methods: {
        sendMessage: function (event: MouseEvent) {
            this.$emit('send-message' ,{ target: this.chat.id, text: this.value })
            this.value = '';
        },

        getClass: function (type: number) {
            if (type == 1)
                return ['sended']
            else
                return ['received']
        }
    }
})

export default class ChatComponent extends Vue { }