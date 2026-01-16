const linksLangs = [
  "https://go.dev/",
  "https://www.php.net/",
  "https://dart.dev/",
  "https://www.python.org/",
  "https://www.typescriptlang.org/",
  "https://nodejs.org/en/",
  "https://www.mysql.com/",
];

const linksFrameworks = [
  "https://reactnative.dev/",
  "https://docs.expo.dev/index.html",
  "https://pt-br.reactjs.org/",
  "https://flutter.dev/docs",
  "https://nextjs.org/",
];

const linksCourses = [
  "https://www.w3schools.com/",
  "https://developer.mozilla.org/",
  "https://www.lingq.com/pt/learn/en/web/library",
  "https://www.duolingo.com/learn",
];

const linksLibs = [
  "https://editorconfig.org/",
  "https://redux.js.org/",
  "https://styled-components.com/",
  "https://reactnavigation.org/",
  "https://sass-lang.com/",
  "https://lesscss.org/",
  "https://styled-icons.js.org/",
  "https://gulpjs.com/",
  "https://eslint.org/",
  "https://fontawesome.com/",
  "https://jestjs.io/",
  "https://prettier.io/",
  "https://realm.io/",
];

const linksLinuxDistros = [
  "https://archlinux.org/",
  "https://www.debian.org/",
  "https://www.gentoo.org/",
  "https://getfedora.org/en/workstation/",
];

const linksJobs = [
  "https://www.fiverr.com/",
  "https://vanhack.com/",
  "https://www.linkedin.com/feed/",
];

const links = [
  "https://graphql.org/",
  "https://docs.docker.com/",
  "https://www.docker.com/",
  "https://git-scm.com/",
  "https://html.spec.whatwg.org/multipage/",
  "https://react-native-async-storage.github.io/async-storage/",
  "https://www.virtualbox.org/",
  "https://www.postgresql.org/",
  "https://testing-library.com/",
  "https://pt.vecteezy.com/",
  "https://www.behance.net/",
  "https://caniuse.com/",
  "https://carbon.now.sh/",
  "https://coolbackgrounds.io/",
  "https://css-tricks.com/",
  "https://dribbble.com/",
  "https://exchangeratesapi.io/",
  "https://freebiesbug.com/",
  "https://rickandmortyapi.com/api/",
  "https://fonts.google.com/icons?selected=Material+Icons",
  "https://undraw.co/illustrations",
  "https://www.img2go.com/",
  "https://layouts1linha.desenvolvimentoparaweb.com/",
  "https://validator.w3.org/",
  "https://www.photopea.com/",
  "https://br.pinterest.com/",
  "https://www.figma.com/",
  "https://www.remove.bg/",
  "http://www.responsinator.com/",
  "https://color.adobe.com/pt/create/color-wheel",
  "https://jigsaw.w3.org/css-validator/",
  "https://simpleicons.org/",
  "https://www.uplabs.com/",
  "https://app.netlify.com/teams/andrierlison/sites",
  "https://coderadio.freecodecamp.org/",
  "https://github.com/",
];

function mountLinks(list) {
  let htmlData = "";

  for (const key in list) {
    htmlData = `<a target="_blank" href="${list[key]}"><li>${list[key]}</li></a>${htmlData}`;
  }

  console.log(htmlData);

  return htmlData;
}

document.getElementById("links-langs").innerHTML = mountLinks(linksLangs);
document.getElementById("links-courses").innerHTML = mountLinks(linksCourses);
document.getElementById("links-frameworks").innerHTML =
  mountLinks(linksFrameworks);
document.getElementById("links-linux-distros").innerHTML =
  mountLinks(linksLinuxDistros);
document.getElementById("links-libs").innerHTML = mountLinks(linksLibs);