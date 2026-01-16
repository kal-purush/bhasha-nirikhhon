export function renderBaseView(content: string) {
	return `
    <!DOCTYPE html>
    <html>
      <body>
        <a href="/"><h1>Jinka ad parser</h1></a>
        ${content}
      </body>
    </html>
  `;
}