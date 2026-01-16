import { VNode } from 'vue';

export interface Picture {
  defaultImage: VNode | string;
  imageWebp: VNode | string;
  imageWebpMobile: VNode | string;
  loadImage: boolean;
}