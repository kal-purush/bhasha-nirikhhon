import React from 'react'

const useHandleScroll = () => {
    const handleScroll = (event: React.MouseEvent<HTMLAnchorElement>, urlNav: string) => {
        if (urlNav.startsWith('#')) {
          event.preventDefault();
          const section = document.querySelector(urlNav);
          if (section) {
            section.scrollIntoView({ behavior: 'smooth' });
          }
        }
      };
  return {
    handleScroll
  }
  }

export default useHandleScroll