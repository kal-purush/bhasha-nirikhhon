/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable no-param-reassign */
import { CSSProperties } from 'vue';

/** 给元素添加样式 */
export const addStyle = (ele: HTMLElement, style: CSSProperties) => {
  Object.entries(style).forEach(([key, value]) => {
    ele.style[key as any] = value;
  });
};

/** 给元素添加类名 */
export const addClass = (ele: HTMLElement, className: string | string[]) => {
  if (typeof className === 'string') {
    ele.classList.add(className);
  } else {
    className.forEach((c) => {
      ele.classList.add(c);
    });
  }
};