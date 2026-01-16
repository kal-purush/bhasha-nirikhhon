interface ICategoryItemIcon{
    url: string;
}

export interface ICategoryItem {
    categories: {
        items: ICategoryItemApi[];
    };
}

export interface ICategoryItemApi{
    icons: ICategoryItemIcon[];
    id: string;
    name: string;
    href: string;
}