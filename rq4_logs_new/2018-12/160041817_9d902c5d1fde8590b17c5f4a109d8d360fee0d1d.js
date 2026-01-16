import Vue from 'vue';
import axios from 'axios';

import { mapState, mapActions } from 'vuex';
import store from '../store/index.js';

const PREFIX = '[ApiService]';

export default new Vue({
  store,
  computed: {
    ...mapState([
      'hostname',
    ]),
    url() { return `http://${this.hostname}:3006`; },
  },
  created() {
  },
  methods: {
    ...mapActions('user', {
      updateUser: 'update',
    }),
    requestCreateUser() {
      return new Promise((resolve, reject) => {
        const url = `${this.url}/api/user`;
        console.log(PREFIX, 'requestCreateUser', url);

        axios.get(url)
          .then(res => {
            const data = res.data;
            console.log(PREFIX, 'requestCreateUser', 'data', data);

            if (data.error) {
              console.error(PREFIX, 'requestCreateUser', 'error', data.error);
              reject(data.error);
              return;
            }

            this.updateUser(data);
            resolve(data);
          });
      });
    },
    requestGetUser(id) {
      return new Promise((resolve, reject) => {
        const url = `${this.url}/api/user/${id}`;
        console.log(PREFIX, 'requestGetUser', url);
        
        axios.get(url)
          .then(res => {
            const data = res.data;
            console.log(PREFIX, 'requestGetUser', 'data', data);

            if (data.error) {
              console.error(PREFIX, 'requestGetUser', 'error', data.error);
              reject(data.error);
              return;
            }

            this.updateUser(data);
            resolve(data);
          })
      });
    },
    requestUpdateUser(payload) {
      return new Promise((resolve, reject) => {
        const url = `${this.url}/api/user`;
        console.log(PREFIX, 'requestUpdateUser', url, payload);

        axios.put(url, payload)
          .then(res => {
            const data = res.data;
            console.log(PREFIX, 'requestUpdateUser', data);

            if (data.error) {
              console.error(PREFIX, 'requestUpdateUser', 'error', data.error);
              reject(data.error);
              return;
            }

            this.updateUser(payload);
            resolve(payload);
          });
      });
    },
  },
});