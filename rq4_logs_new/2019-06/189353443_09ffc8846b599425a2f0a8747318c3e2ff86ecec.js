Vue.component('modalCreateMonth', {
    props: ['dateId'],    data: function() {
        return {
            bodyText: 'Создать новый месяц?',
            warning: false,
            ignoreWarning: false
        }
    },
    template:
    '<div v-if="this.$parent.showModalCreateMonth" class="modal">'
        + '<transition name="slideIn" appear>'
            + '<div class="modal-content new-month">'
                + '<div @click="closeModal()" class="modal-button close">×</div>'
                + '<p>'
                    + '{{ bodyText }}<br />'
                    + '<button class="new-month-button no" @click="closeModal()">NO</button> <button @click="createNewMonth()" class="new-month-button yes">YES</button><br /><br />'
                    + '<span class="new-month-warning" v-if="warning"><input id="warning" v-model="ignoreWarning" type="checkbox" /> <label for="warning">Игнорировать предупреждение</label></span>'
                + '</p>'
            + '</div>'
        + '</transition>'
    + '</div>',
    methods: {
        closeModal: function () {
            this.$parent.showModalCreateMonth = false;
            this.ignoreWarning = this.warning = false;
            this.bodyText = 'Создать новый месяц?';
        },
        createNewMonth: function () {
            if (!this.warning){
                axios.get('month/checkBeforeCreateNewMonth?dateId=' + this.dateId)
                    .then(result => {
                        switch (result.data) {
                            case 'MONTH_OK.FULL_NOT':
                                this.bodyText = 'Платежи по текущему месяцу внесены не до конца, продолжить?';
                                this.warning = true;
                                break;
                            case 'MONTH_NOT.FULL_OK':
                                this.bodyText = 'Календарный месяц еще не завершен, продолжить?';
                                this.warning = true;
                                break;
                            case 'MONTH_NOT.FULL_NOT':
                                this.bodyText = 'Календарный месяц еще не завершен, платежи по текущему месяцу внесены не до конца, продолжить?';
                                this.warning = true;
                                break;
                        }
                    })
            } else if (this.ignoreWarning){
                axios.put('month/createNewMonthByDateId?dateId=' + this.dateId)
                    .then(result => {
                        this.$parent.monthList = result.data;
                        this.closeModal();
                    })
            }

        }
    }
});

Vue.component('templateHaveNoticesButton', {
    props: ['monthlySpendId'],
    data: function() {
        return {
            showNoticeModal: false,
            notices: []
        }
    },
    template:
    '<span>'
        + '<button @click="showNoticeModalFunc()" class="month-notices-show-button" > </button>'
        + '<div v-if="showNoticeModal" class="modal">'
            + '<transition name="slideIn" appear>'
                + '<div class="modal-content notice">'
                    + '<div @click="closeModal()" class="modal-button close">×</div>'
                    + '<div v-if="notices.length > 0" v-for="notice in notices">'
                        + '<p>{{ notice.text }}</p>'
                    + '</div>'
                + '</div>'
            + '</transition>'
        + '</div>'
    + '</span>',
    methods: {
        showNoticeModalFunc: function () {
            this.showNoticeModal = true;
            axios.get('notices/getByMonthlySpendsId/' + this.monthlySpendId).then(result => {
                this.notices = result.data;
            });
        },
        closeModal: function () {
            this.showNoticeModal = false;
        }
    }
});

Vue.component('modalCreateTemplate', {
    props: ['dateId'],
    data: function() {
        return {
            name: '',
            bodyText: 'Имя шаблона:'
        }
    },
    template:
    '<div v-if="this.$parent.showModalCreateTemplate" class="modal">'
        + '<transition name="slideIn" appear>'
            + '<div class="modal-content new-template">'
                + '<div @click="closeModal()" class="modal-button close">×</div>'
                + '{{ bodyText }}'
                + '<p><input v-model="name" /> <button class="save-button" @click="saveTemplateList()"> </button></p>'
            + '</div>'
        + '</transition>'
    + '</div>',
    methods: {
        closeModal: function () {
            this.name = '';
            this.bodyText = 'Имя шаблона:';
            this.$parent.showModalCreateTemplate = false;
        },
        saveTemplateList: function () {
            if (this.dateId > 0 && this.name.length > 1){
                axios.put('templatesList/createTemplatesListByMonth?dateId=' + this.dateId + '&name=' + this.name)
                    .then(() => {
                        this.bodyText = 'OK';
                        let self = this;
                        setTimeout(function(){
                            self.closeModal();
                        }, 1000);
                    })
            }

        }
    }
});

Vue.component('notice', {
    props: ['monthlySpendsId'],
    data: function() {
        return {
            text: null,
            bodyText: 'Текст заметки:',
            isError: false,
            remind: false
        }
    },
    template:
    '<div v-if="this.$parent.showNoticeModal" class="modal">'
        + '<transition name="slideIn" appear>'
            + '<div class="modal-content notice">'
                + '<div @click="closeModal()" class="modal-button close">×</div>'
                + '<p>'
                    + '{{ bodyText }}<br />'
                    + '<textarea v-model="text" ></textarea>'
                    + '<input id="is-remind" v-model="remind" type="checkbox" /> <label for="is-remind">Remind</label> <button @click="saveNotice()">Сохранить</button>'
                + '</p>'
            + '</div>'
        + '</transition>'
    + '</div>',
    methods: {
        closeModal: function () {
            this.$parent.showNoticeModal = false;
            this.text = null;
            this.bodyText = 'Текст заметки:';
            this.isError = false;
            this.remind = false;
        },
        saveNotice: function () {
            axios.put('notices/add?monthlySpendId=' + this.monthlySpendsId + '&text=' + this.text + '&remind=' + this.remind)
                .then(() => {
                    this.bodyText = 'OK';
                    let self = this;
                    setTimeout(function(){
                        self.closeModal();
                    }, 1000);
                })
        }
    }
});

Vue.component('modalNoMonth', { // модальное окно с сообщением о том, что текущего месяца нет
    template:
    '<div v-if="this.$parent.showModal == true && (enabledTemplatesHasFound || previousMonthHasFound)" id="modal-template">'
        + '<div class="modal-mask"><div class="modal-wrapper"><div class="modal-container">'
            + '<div class="modal-header">'
                + '<slot name="header">Внимание, текущий месяц не найден!</slot>'
            + '</div>'
            + '<div class="modal-body">'
                + '<slot name="body">Создай новый месяц по активному шаблону или по предыдущему месяцу:</slot>'
            + '</div>'
            + '<div class="modal-footer">'
                + '<slot name="footer">'
            + '<button @click="closeModal()">X</button> <button :disabled="!enabledTemplatesHasFound" @click="createMonthByEnabled()">by Enabled</button>'
                + '&nbsp;'
                + '<button :disabled="!previousMonthHasFound" @click="createMonthByLast()">by Last</button>'
                + '</slot>'
            + '</div>'
        + '</div></div></div>'
    + '</div>',
    data: function() {
        return {
            enabledTemplatesHasFound: false,
            previousMonthHasFound: false
        }
    },
    methods: {
        closeModal: function () {
            this.$parent.showModal = false;
        },
        createMonthByEnabled: function () {
            if (this.enabledTemplatesHasFound){
                let tmpList = [];
                axios.get('month/createFromEnabled').then(result =>
                    // result.data.forEach(month => {
                    //     tmpList.push(month);
                    this.$parent.monthList = result.data
                    // })
                );
                this.$parent.showModal = false;
                // this.$parent.monthList = tmpList;
            }
        },
        createMonthByLast: function () {
            if (this.previousMonthHasFound) {
                var tmpList = [];
                axios.get('month/createFromLastMonth').then(result =>
                    this.$parent.monthList = result.data
                    // result.data.forEach(month => {
                    //     tmpList.push(month);
                    // })
                );
                this.$parent.showModal = false;
                // this.$parent.monthList = tmpList;
            }
        }
    },
    created: function () {
        axios.get('templatesList/getEnabledTemplate').then( // если найден шаблон с полем enabled = true, то
            response => {
                if(response.data.id){
                    this.enabledTemplatesHasFound = true // сделать кнопку "Заполнить по активному шаблону" активной
                }
            }
        ).catch(
            error => this.enabledTemplatesHasFound = false
        );
        axios.get('month/all').then( // если найдены предыдущие месяцы, то
            response => {
                if(response.data.length > 0){
                    this.previousMonthHasFound = true // сделать кнопку "Заполнить по предыдущему месяцу" активной
                }
            }
        ).catch(
            error => this.previousMonthHasFound = false
        );
    }
});