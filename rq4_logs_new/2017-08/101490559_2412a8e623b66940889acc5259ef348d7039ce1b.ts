import Vue from 'vue';
import { createClient } from 'contentful-management';

const client = createClient({
  accessToken: 'CFPAT-2140842295f12ba8a531ba6052de1eaefdd2796320ed38d150345bd4621899c0',
});

export default Vue.extend({
  template: `
    <div>
      <div v-if="show">
        Have a story to share? Please do!
        <textarea v-model="story"></textarea>

        <button @click="submitStory()">Submit my story</button>
      </div>
      <div v-if="!show">
        Thanks for submitting your story!
      </div>
    </div>
  `,
  props: ['placeId'],
  data() {
    return {
      story: '',
      show: true,
    };
  },
  methods: {
    async submitStory() {
      const space = await client.getSpace('881sco8a7hxb');
      await space.createEntry('story', {
        fields: {
          placeId: {
            'en-US': {
              sys: {
                type: 'Link',
                linkType: 'Entry',
                'id': this.placeId.sys.id,
              },
            },
          },
          story: {
            'en-US': this.story,
          },
        },
      });

      this.show = false;
    }
  },
});