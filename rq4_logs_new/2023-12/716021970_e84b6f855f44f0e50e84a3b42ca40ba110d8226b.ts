import { sStorage } from '@/utils';

interface TagsState {
  tags: RouteTag[];
}

export const usetTagsStore = defineStore('tags', {
  state: (): TagsState => ({
    tags: [],
  }),
  actions: {
    setTags(tags: RouteTag[]) {
      this.tags = tags;
      sStorage.set('tags', tags);
    },
    addTag(tag: RouteTag) {
      const findItem = this.tags.find((item) => item.path === tag.path);
      if (!findItem) {
        this.setTags([...this.tags, tag]);
      }
    },
    removeTag({ path }: RouteTag.path) {
      this.setTags(this.tags.filter((tag) => tag.path !== path));
    },
  },
});