import { prime_number_ } from "~/pages/matek/prime.vue";

export const useLkkt = (numbers: number[]): number[][] => {
	const felbontott: number[][] = [[], []];

	for (const element of numbers) {
		const egyeske = prime_number_(element, true);
		console.log(egyeske);
		if (egyeske) {
			for (let i = 0; i < egyeske.result[0].length; i++) {
				const element = egyeske.result[0][i];

				if (felbontott[0].includes(element)) {
					if (
						felbontott[1][felbontott[0].indexOf(element)] < egyeske.result[1][i]
					) {
						felbontott[1][felbontott[0].indexOf(element)] =
							egyeske.result[1][i];
					}
				} else {
					felbontott[0].push(egyeske.result[0][i]);
					felbontott[1].push(egyeske.result[1][i]);
				}
			}
		}
	}
	return felbontott;
};