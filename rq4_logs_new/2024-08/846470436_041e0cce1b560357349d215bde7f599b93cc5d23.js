export const scrollDesktop = id => {
	const section = document.getElementById(id);
	const offset = 81;

	if (section) {
		const yOffset = -offset;
		const y = section.getBoundingClientRect().top + window.scrollY + yOffset;

		window.scrollTo({ top: y, behavior: "smooth" });
	}
};

export const scrollMobile = id => {
	const section = document.getElementById(id);
	const offset = 65;

	if (section) {
		const yOffset = -offset;
		const y = section.getBoundingClientRect().top + window.scrollY + yOffset;

		window.scrollTo({ top: y, behavior: "smooth" });
	}
};