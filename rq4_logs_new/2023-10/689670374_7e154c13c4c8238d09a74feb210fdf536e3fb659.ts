export type dataType = {
  profile: profileType;
  content: contentType;
};

export type profileType = {
  name: {
    en: string;
    ja: string;
  };
  nickname: string;
  occupation: {
    en: string;
    ja: string;
  };
  date: {
    birth: string;
    engineering: string;
  };
  sns: {
    github: string;
    x: string;
    instagram: string;
  };
  link: string[];
};

export type contentType = {
  aboutMeData: aboutMeType;
  favoriteData: favoriteType;
  enhancingData: enhancingType;
  skillData: skillType;
  projectData: projectType;
  portfolioData: portfolioType;
  careerData: careerType;
  experienceData: experienceType;
};

export type aboutMeType = {
  en: string;
  ja: string;
};

export type favoriteType = string[];

export type enhancingType = string[];

export type skillType = skillItemType[];

export type skillItemType = {
  sectionTitle: string;
  skillSet: skillSetType[];
};

export type skillSetType = {
  icon: string;
  name: string;
  rate: number;
};

export type projectType = projectItemType[];

export type projectItemType = {
  title: {
    en: string;
    ja: string;
  };
  description: {
    en: string;
    ja: string;
  };
  technology: string[];
  completion_date: string;
};

export type portfolioType = portfolioItemType[];

export type portfolioItemType = {
  name: string;
  description: string;
  link: string;
  technology: string[];
};

export type careerType = careerItemType[];

export type careerItemType = {
  name: {
    en: string;
    ja: string;
  };
  description: {
    en: string;
    ja: string;
  };
  join: string;
  leave: string;
  link: string;
};

export type experienceType = {
  technology: string;
  application: string;
};