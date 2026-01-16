export interface MenuLink {
	id: number;
	name: string;
	link: string;
	submenu: Array<{ name: string; link: string }>;
}

export interface MobileMenuProps {
	menuOpen: boolean;
	setMenuOpen: React.Dispatch<React.SetStateAction<boolean>>;
	menuLinks: MenuLink[];
	subMenuOpen: boolean;
	toggle: number;
	handleSubmenu: any;
}

export interface NavBarProps {
	menuLinks: MenuLink[];
	setMenuOpen: React.Dispatch<React.SetStateAction<boolean>>;
	menuOpen: boolean;
}