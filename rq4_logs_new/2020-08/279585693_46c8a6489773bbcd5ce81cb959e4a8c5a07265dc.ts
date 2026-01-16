import React from 'react';

type P = {
	target: React.RefObject<Element>;
	onIntersect: () => void;
	root?: React.RefObject<Element>;
	threshold?: number | number[];
	rootMargin?: string;
	enabled?: boolean;
};

export default function useIntersectionObserver({
	root,
	target,
	onIntersect,
	threshold = 1,
	rootMargin = '0px',
	enabled = true,
}: P) {
	React.useEffect(() => {
		const el = target && target.current;
		if (!enabled || !el) {
			return;
		}

		const observer = new IntersectionObserver(entries => entries.forEach(entry => entry.isIntersecting && onIntersect()), {
			root: root && root.current,
			rootMargin,
			threshold,
		});

		observer.observe(el);

		return () => {
			observer.unobserve(el);
		};
	}, [enabled, onIntersect, root, threshold, rootMargin, target]);
}