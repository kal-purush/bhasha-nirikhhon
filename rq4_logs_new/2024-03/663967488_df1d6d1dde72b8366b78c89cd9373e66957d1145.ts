export const toggleTheme = () => {
  const htmlTag = document.querySelector("html");

  if (htmlTag?.classList.contains("light")) {
    htmlTag.setAttribute('class', 'dark');
    localStorage.setItem('theme', 'dark')
  } else {
    htmlTag?.setAttribute('class', 'light');
    localStorage.setItem('theme', 'light')
  }
};

export const getTheme = (): string => {
  const theme = localStorage.getItem('theme')
  return theme ? theme : "dark"
}

export const setStartTheme = () => {
  const htmlTag = document.querySelector("html");

  if (localStorage.getItem("theme") === 'light') {
    htmlTag?.setAttribute('class', 'light');
  } else {
    htmlTag?.setAttribute('class', 'dark');
  }
}