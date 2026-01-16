const NAVIGATION = [
  {
    label: "Գլխավոր",
    path: "/",
  },
  {
    label: "Նորություններ",
    path: "/news",
    children: [
      {
        label: "Հայկական",
        path: "/news/armenian",
        children: [
          {
            label: "Ստարտափներ",
            path: "/news/armenian/startups",
          },
          {
            label: "Հիմնադիրներ",
            path: "/news/armenian/founders",
          },
        ],
      },
      {
        label: "Համաշխարհային",
        path: "/news/global",
        children: [
          {
            label: "Կազմակերպություններ",
            path: "/news/global/organizations",
          },
          {
            label: "Տեխ Լիդերներ",
            path: "/news/global/tech-leaders",
          },
          {
            label: "Ստարտափներ",
            path: "/news/global/startups",
          },
        ],
      },
    ],
  },
  {
    label: "Դասընթացներ",
    path: "/courses",
    icon: "courses",
    children: [
      {
        label: "Անհատներ",
        path: "/courses/individuals",
      },
      {
        label: "Կազմակերպություններ",
        path: "/courses/organizations",
      },
    ],
  },
  {
    label: "Միջոցառումներ",
    path: "/events",
  },
  {
    label: "Օգտակար Գործիքներ",
    path: "/tools",
    icon: "tools",
  },
  {
    label: "Մեր Մասին",
    path: "/about",
    children: [
      {
        label: "Կապ",
        path: "/about/contact",
      },
      {
        label: "Թիմ",
        path: "/about/team",
      },
      {
        label: "Օգտագործման Պայմաններ",
        path: "/about/terms",
      },
    ],
  },
  {
    label: "Ռեսուրսներ",
    path: "/resources",
    children: [
      {
        label: "AI Tools",
        path: "/resources/ai-tools",
      },
      {
        label: "Ուսումնական ռեսուրսներ",
        path: "/about/educational-resources",
      },
    ],
  },
];

export default NAVIGATION;