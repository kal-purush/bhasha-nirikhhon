function createSkill(iconPath, name) {
  const skillCard = document.createElement("div");
  skillCard.classList.add("skill-card");

  const icon = document.createElement("img");
  icon.classList.add("icon");
  icon.setAttribute("src", iconPath);
  icon.setAttribute("alt", "");
  icon.setAttribute("loading", "lazy");

  const skill = document.createElement("p");
  skill.classList.add("skill-title");
  skill.textContent = name;

  skillCard.appendChild(icon);
  skillCard.appendChild(skill);

  return skillCard;
}

function createSection(name, skillsArray) {
  const Section = document.createElement("div");
  Section.classList.add("skills-section");

  const Title = document.createElement("h3");
  Title.textContent = name;

  const skills = document.createElement("div");
  skills.classList.add("skills-cards");
  skillsArray.forEach((skill) => {
    skills.appendChild(createSkill(skill[0], skill[1]));
  });

  Section.appendChild(Title);
  Section.append(skills);

  return Section;
}

const dataSkills = [
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg",
    "python",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/pandas/pandas-original.svg",
    "pandas",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/jupyter/jupyter-original.svg",
    "jupyter",
  ],
  [
    "https://upload.wikimedia.org/wikipedia/commons/0/05/Scikit_learn_logo_small.svg",
    "scikit-learn",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/tensorflow/tensorflow-original.svg",
    "tensorflow",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg",
    "sql",
  ],
];

const frontEndSkills = [
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/javascript/javascript-original.svg",
    "javascript",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/css3/css3-original.svg",
    "css",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/html5/html5-original.svg",
    "html",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/react/react-original.svg",
    "react",
  ],
];

const miscSkills = [
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/git/git-original.svg",
    "git",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/bash/bash-original.svg",
    "bash",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/npm/npm-original-wordmark.svg",
    "npm",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/webpack/webpack-original.svg",
    "webpack",
  ],
  [
    "https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-original.svg",
    "docker",
  ],
];

function loadSkills() {
  const skills = document.getElementById("skills-container");

  skills.append(createSection("Data Science", dataSkills));
  skills.append(createSection("Front-End", frontEndSkills));
  skills.append(createSection("Miscellaneous", miscSkills));
}

export default loadSkills;