export default {
  namespaced: true,

  state: {
    formSubmitted: false,
    formProcess: false,
    order: [],
    formFields: [
      {
        name: 'Name',
        value: 'John',
        pattern: /^[a-zA-Z ]{2,30}$/,
        status: false
      },
      {
        name: 'Surname',
        value: '',
        pattern: /^[a-zA-Z ]{2,30}$/,
        status: false
      },
    ],
  },

  getters: {
    formFields: (state) => state.formFields,

    name: (state) => state.formFields[0].status ? state.formFields[0].value : '',

    formSubmitted: (state) => state.formSubmitted,

    formProcess: (state) => state.formProcess,

    amountValidFields(state) {
      let amount = 0;

      state.formFields.forEach((control) => {
        if (control.status) amount++;
      });

      return amount;
    }
  },

  mutations: {
    setFormFieldsValue(state, payload) {
      state.formFields[payload.ndx].value = payload.value;
    },

    setFormFieldsStatus(state, payload) {
      state.formFields[payload.ndx].status = payload.status;
    },

    startSubmit(state) {
      state.formProcess = true;
    },

    submitted(state) {
      state.formSubmitted = true;
      state.formProcess = false;
    },

    setOrder(state, products) {
      state.order = products;
    }
  },

  actions: {
    setFormFieldsValue(store, payload) {
      store.commit('setFormFieldsValue', payload);
    },

    setFormFieldsStatus(store, payload) {
      store.commit('setFormFieldsStatus', payload);
    },

    sendOrder(store, products) {
      store.commit('startSubmit');


      setTimeout(() => {
        store.commit('submitted');
        store.commit('setOrder', products);
      }, 1000);
    }
  }
}