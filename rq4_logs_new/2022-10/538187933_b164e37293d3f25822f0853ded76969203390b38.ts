export interface IListItems {
  id: string;
}

export interface IFooterButtons {
  title: string;
  href: string;
  iconClassName?: string;
  children: React.ReactNode;
}

export interface INavbar {
  setIsOpenDrawer?: React.Dispatch<React.SetStateAction<boolean>>;
  customClassname: string;
}

export interface INavbarList {
  setIsOpenDrawer?: React.Dispatch<React.SetStateAction<boolean>>;
}