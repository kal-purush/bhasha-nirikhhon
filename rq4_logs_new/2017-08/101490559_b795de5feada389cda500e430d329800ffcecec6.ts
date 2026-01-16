import Vue from 'vue';
import { createClient } from 'contentful';

import { get, sample } from 'lodash';

const client = createClient({
  space: '881sco8a7hxb',
  accessToken: '41354290dd34a67e7c1f04c8ef22d1be8f9bc402bec4b42bb002cc8b340b200a',
});

export default Vue.extend({
  template: `
    <div>
      <h1>Get a Hint</h1>
      <div v-if="hintList && !error">
        <ul id="hintList">
          <li v-for="hint in hintList">
            <a>{{ hint.fields.body }}</a>
          </li>
        </ul>
      </div>
      

      <div v-if="error">
        Sorry, but that doesn't seem to be a hint!
      </div>
    </div>
  `,
  created() {
    this.fetchData();
  },
  data() {
    return {
      error: null,
      hintList: null,
    };
  },
  watch: {
    $route(to, from) {
      this.fetchData();
    },
  },
  methods: {
    async fetchData() {

      try {
        const hintList = await client.getEntries({
          content_type: 'hint'
        })

        console.log(hintList);

        this.error = false;
        this.hintList = hintList.items;
      }
      catch (err) {
        this.error = true;
      }
    },
  },
});