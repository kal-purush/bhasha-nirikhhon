export function hello(name) {
  return `Hello, ${name}!`;
}

document.getElementById('main').innerText = hello('webpack');