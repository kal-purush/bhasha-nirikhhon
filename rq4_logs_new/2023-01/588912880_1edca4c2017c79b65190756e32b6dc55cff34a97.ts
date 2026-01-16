const reactSpringBottomSheet = {
  ':root': {
    '--rsbs-backdrop-bg': 'rgba(5, 7, 50, 0.7)',
    '--rsbs-bg': '#0E2144',
    '--rsbs-handle-bg': '#707F95',
    '--rsbs-max-w': 'auto',
    '--rsbs-ml': 'env(safe-area-inset-left)',
    '--rsbs-mr': 'env(safe-area-inset-right)',
    '--rsbs-overlay-rounded': '16px',
  },
  '[data-rsbs-scroll]': {
    height: '100%',
  },
  '[data-rsbs-overlay]': {
    height: 'calc(100% - 80px)',
  },
  '[data-rsbs-root]': {
    position: 'fixed',
    zIndex: 1000,
  },
  '[data-rsbs-backdrop]': {
    backdropFilter: 'blur(6.5px)',
  },
  '[data-rsbs-header]': {
    height: '40px',
    pos: 'relative',
    '&:before': {
      position: 'absolute',
      top: '50%',
      transform: 'translate(-50%, -50%)',
      borderRadius: '10px',
      width: '45px',
    },
  },
  '[data-rsbs-content]': {
    height: '100%',
  },
};

export default reactSpringBottomSheet;