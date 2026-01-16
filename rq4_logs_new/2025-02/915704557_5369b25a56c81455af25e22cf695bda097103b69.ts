interface IFooterLogo {
  imageUrl: string;
  imageLabel: string;
}

interface IFooterItems {
  label: string;
  items: string[];
  id: number;
}

interface IFooterSocialMediaIcons {
  imageUrl: string;
  imageLabel: string;
  id: number;
}

interface IDownloadApp {
  imageUrl: string;
  imageLabel: string;
  id: number;
}

export const footerLogo: IFooterLogo = {
  imageUrl: "/images/logo/logo.png",
  imageLabel: "Logo",
};

export const footerItems: IFooterItems[] = [
  {
    label: "Services",
    items: ["Get a listing", "Get a listing", "Help and Contact"],
    id: 1,
  },
  {
    label: "Discover",
    items: [
      "Eating Out",
      "Home Services",
      "Car Services",
      "Deals",
      "Explore",
      "Find a Business",
    ],
    id: 2,
  },
  {
    label: "K Blog",
    items: [
      "Blog Home",
      "Trades",
      "Hot Deals",
      "Explore London",
      "Most popular in Winter",
      "Promote a Business",
    ],
    id: 3,
  },
  {
    label: "Company",
    items: [
      "Digital Marketing Solutions",
      "Find Your Solution",
      "Contact",
      "About",
    ],
    id: 4,
  },
  {
    label: "Info",
    items: [
      "Privacy Policy",
      "Cookie Policy",
      "Conditions of use",
      "Reviews policy",
      "Modern Slavery Act Statement",
    ],
    id: 5,
  },
];

export const socialMediaIcons: IFooterSocialMediaIcons[] = [
  {
    imageUrl: "/icons/dashboard/footer/instagram.svg",
    imageLabel: "Instagram",
    id: 1,
  },
  {
    imageUrl: "/icons/dashboard/footer/x.svg",
    imageLabel: "X",
    id: 2,
  },
  {
    imageUrl: "/icons/dashboard/footer/facebook.svg",
    imageLabel: "Facebook",
    id: 3,
  },
];

export const downloadApp: IDownloadApp[] = [
  {
    imageUrl: "/icons/dashboard/footer/download/playmarket.svg",
    imageLabel: "Play market download",
    id: 1,
  },
  {
    imageUrl: "/icons/dashboard/footer/download/appstore.svg",
    imageLabel: "App store download",
    id: 2,
  },
];