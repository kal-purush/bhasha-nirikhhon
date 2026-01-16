import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Highlight } from 'ngx-highlightjs';

@Component({
  selector: 'app-doc-search-bar',
  standalone: true,
  imports: [CommonModule, Highlight],
  templateUrl: './doc-search-bar.component.html',
  styleUrl: './doc-search-bar.component.scss',
})
export class DocSearchBarComponent {
  import = `import '../node_modules/ip-search-bar/dist/ip-search-bar/ip-search-bar.esm';`;

  example = `<ip-search-bar
  placeholder="Please enter a keyword !"
  suggestions-data='["Avocat ", "Amande", "Abricot", "Ananas"]'
>
</ip-search-bar>`;

  custom1 = `ip-search-bar {
  --text-btn-color: #006342;
  --font-size: 15px;
}`;
  event = `const searchBar = document.querySelector('ip-search-bar');

searchBar.addEventListener('buttonClicked', (event) => {
  console.log('Search button clicked!');
});`;
}