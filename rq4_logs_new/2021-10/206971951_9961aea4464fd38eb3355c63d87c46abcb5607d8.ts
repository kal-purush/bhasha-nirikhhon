import { AnimatePresence, motion } from 'framer-motion';
import { createElement } from 'react';

type BackdropProps = {
  visible?: boolean;
} & Omit<typeof motion.div, '$$typeof'>;

function Backdrop(props: BackdropProps) {
  const { visible, ...args } = props;
  return createElement(
    AnimatePresence,
    { initial: false },
    visible &&
      createElement(motion.div, {
        style: {
          position: 'fixed',
          width: '100vw',
          height: '100vh',
          left: 0,
          top: 0,
          zIndex: 1000,
        },
        initial: 'invisible',
        animate: 'visible',
        exit: 'invisible',
        variants: {
          invisible: { backgroundColor: 'rgba(0, 0, 0, 0)' },
          visible: { backgroundColor: 'rgba(0, 0, 0, 0.75)' },
        },
        transition: { duration: 0.3, type: 'spring' },
        ...args,
      })
  );
}

export default Backdrop;