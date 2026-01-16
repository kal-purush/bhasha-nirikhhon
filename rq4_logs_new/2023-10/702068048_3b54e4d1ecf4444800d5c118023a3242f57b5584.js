function convertToLinks() {
  const inputText = document.getElementById('inputText').value;
  const outputLinks = document.getElementById('outputLinks');


  const lines = inputText.split('\n');

  
  outputLinks.innerHTML = '';
  lines.forEach(function (line, index) {
    const trimmedLine = line.trim();
    if (trimmedLine !== '') {
      const linkElement = document.createElement('a');
      linkElement.href = trimmedLine;
      linkElement.textContent = `Link ${index + 1}`;
      outputLinks.appendChild(linkElement);

      const lineBreak = document.createElement('br');
      outputLinks.appendChild(lineBreak);
    }
  });
}