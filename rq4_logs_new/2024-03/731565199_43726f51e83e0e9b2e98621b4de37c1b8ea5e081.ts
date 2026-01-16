import { ANIMATION_STYLE_MAP } from './../constants/Animation';
import { ANIMATION_TYPE } from '../constants/Animation';
import { ObjValues } from '../types/ObjValues';

const getAnimationStyle = (
  animationType: ObjValues<typeof ANIMATION_TYPE>,
  animationDuration: number,
  animationTrigger: boolean,
) => {
  if (animationType === ANIMATION_TYPE.NONE) return {};

  const animationCss = {
    [ANIMATION_STYLE_MAP[animationType]]: animationTrigger ? 1 : 0,
  };
  const transitionCss = {
    transition: `${animationDuration}ms`,
  };
  return Object.assign({}, animationCss, transitionCss);
};

export default getAnimationStyle;