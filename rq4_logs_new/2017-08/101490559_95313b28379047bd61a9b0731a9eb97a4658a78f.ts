import Vue from 'vue';
import { createClient } from 'contentful';

const client = createClient({
  space: '881sco8a7hxb',
  accessToken: '41354290dd34a67e7c1f04c8ef22d1be8f9bc402bec4b42bb002cc8b340b200a',
});

export default Vue.extend({
  template: `
    <div>
      <div v-if="place && !error">
        {{place.name}}
      </div>

      <div v-if="error">
        Sorry, but that doesn't seem to be a place!
      </div>
    </div>
  `,
  created() {
    this.fetchData();
  },
  data() {
    return {
      error: null,
      place: null,
    };
  },
  methods: {
    async fetchData() {
      try {
        const place = await client.getEntry(this.$route.params.id);
        this.place = {
          name: place.fields.name,
        };
      }
      catch (err) {
        this.error = true;
      }
    },
  },
});
