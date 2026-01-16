namespace Parsers {
	module.exports = (selector: string): Array<Github.Developer> => {
		const SITE = 'https://www.github.com',
			$items: NodeListOf<HTMLElement> = document.querySelectorAll(selector);

		return Array.from($items).map((item: HTMLElement): Github.Developer => {

			const $links: NodeListOf<HTMLElement> = item.querySelectorAll('div a'),
				$img = $links[1].querySelector('img').getAttribute('src'),
				$homePage = SITE + $links[2].getAttribute('href'),
				$title = $links[2].textContent.trim().replace(/[\r|\n|\s]/g, ''),
				$repository = SITE + $links[3].getAttribute('href'),
				$summary = $links[3].querySelectorAll('span[class^="repo-snipit"]');


			return {
				title: $title,
				rank: $links[0].textContent.trim(),
				avatar: $img,
				homePage: $homePage,
				repository: $repository,
				repoName: $summary[0].textContent.trim(),
				repoDesc: $summary[1].textContent.trim(),
			};
		});
	};
}