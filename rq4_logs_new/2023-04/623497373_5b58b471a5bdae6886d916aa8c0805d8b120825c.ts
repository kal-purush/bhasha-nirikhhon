import { usePathname } from 'next/navigation';
import { useState } from 'react';

const useHeaderMobileVM = () => {
  const [ isShowMenu, setIsShowMenu ] = useState(false);
  const pathname = usePathname()

  const handleClickHamburgerMenu = () => setIsShowMenu(prev => !prev)
  return {
    isShowMenu,
    pathname,
    handleClickHamburgerMenu
  }
}

export default useHeaderMobileVM;