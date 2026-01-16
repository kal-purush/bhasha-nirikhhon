declare namespace JSX {
  interface IntrinsicElements {
    urlset: React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement> & {
      xmlns: string;
    };
    url: React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
    loc: React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
    lastmod: React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
    changefreq: React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
    priority: React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
  }
}