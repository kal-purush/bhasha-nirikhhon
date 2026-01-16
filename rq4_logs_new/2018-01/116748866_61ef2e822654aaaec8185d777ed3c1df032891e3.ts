import { animate, AnimationEntryMetadata, state, style, transition, trigger } from '@angular/core';

// Component transition animations
//导出了slideInDownAnimation常量，并将它设置为*routeAnimation动画触发器
export const slideInDownAnimation: AnimationEntryMetadata =
  trigger('routeAnimation', [
    state('*',//指定了通配符状态*，能匹配该路由组件存在时的任何动画状态
      style({
        opacity: 1,
        transform: 'translateX(0)'
      })
    ),
    transition(':enter', [//定义动画效果enter
      style({
        opacity: 0,
        transform: 'translateX(-100%)'
      }),
      animate('0.2s ease-in')//在组件进入应用时从屏幕左侧缓慢进入
    ]),
    transition(':leave', [//定义动画效果leave
      animate('0.5s ease-out', style({
        opacity: 0,
        transform: 'translateY(100%)'//在组件离开应用视图时让它向下飞出
      }))
    ])
  ]);