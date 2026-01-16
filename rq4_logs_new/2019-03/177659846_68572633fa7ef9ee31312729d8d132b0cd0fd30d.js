import './styles.css';

function isTouch() {
	return !!(
		'ontouchstart' in window ||
		(window.DocumentTouch && document instanceof window.DocumentTouch) ||
		navigator.maxTouchPoints > 0 ||
		navigator.msMaxTouchPoints > 0
	);
}
const touchDevice = isTouch();

if (touchDevice) {
	document.getElementsByTagName('html')[0].classList.add('touch');
}

if (!touchDevice) {
	const debouncedResize = debounce(handleResizeEvent, 100);

	window.removeEventListener('resize', debouncedResize);
	window.addEventListener('resize', debouncedResize);

	const leftPaddle = '.chapternav-paddle-left';
	const rightPaddle = '.chapternav-paddle-right';
	const startOfList = 'left';
	const endOfList = 'right';
	const items = Array.from(document.querySelectorAll('.chapternav-item'));
	const navItems = document.querySelector('.chapternav-items');
	const itemPadding = 40;
	const itemsWidth = items.reduce(function(sum, item) {
		return sum + item.clientWidth + itemPadding;
	}, 0);
	// In the future build this list out of items array
	const observedItems = items
		.map(function(item, index) {
			return {
				name: item.textContent.replace(/\s/g, ''),
				visible: false,
				index
			};
		})
		.reduce(function(list, current) {
			return { ...list, [current.name]: current };
		}, {});
	// Object to track which request animation frame callbacks are active
	const rafTickers = {
		resizeTicker: true,
		hidePreviousPaddle: false,
		hideNextPaddle: false,
		showPreviousPaddle: false,
		showNextPaddle: false
	};

	let width = window.innerWidth;

	requestAnimationFrame(updatePaddles);

	items.forEach(function(item) {
		setObserversOnItem(item);
	});

	function handleResizeEvent(e) {
		if (
			(width < itemsWidth && window.innerWidth >= itemsWidth) ||
			(width >= itemsWidth && window.innerWidth < itemsWidth)
		) {
			updatePaddles();
		}

		width = window.innerWidth;
	}

	function handlePaddleClick(e) {
		const forward = e.target.classList.contains('chapternav-paddle-right');
		// Number of items to scroll
		const nextGroupLength = numItemsVisible();
		const observedArray = Object.values(observedItems);
		const visibleItems = observedArray
			.filter(function(value) {
				return value.visible;
			})
			.sort(function(a, b) {
				return a.index - b.index;
			});
		// Probably need to use this for something related to scrolling the list
		let hiddenItemIndex;

		if (forward) {
			const lastVisibleIndex = visibleItems[visibleItems.length - 1].index;
			const hiddenItems = observedArray
				.filter(function(value) {
					return !value.visible && value.index > lastVisibleIndex;
				})
				.sort(function(a, b) {
					return a.index - b.index;
				});

			if (hiddenItems.length < nextGroupLength) {
				// Only scroll the length of hidden items
				const lastHiddenItem = hiddenItems[hiddenItems.length - 1];
				items[lastHiddenItem.index].scrollIntoView({ behavior: 'smooth' });
				hiddenItemIndex = lastHiddenItem.index;
			} else {
				// Scroll the length of nextGroupLength
				// Only scroll the length of hidden items
				const lastHiddenItem = hiddenItems[nextGroupLength - 1];
				items[lastHiddenItem.index].scrollIntoView({ behavior: 'smooth' });
				hiddenItemIndex = lastHiddenItem.index;
			}
		} else {
			const firstVisibleIndex = visibleItems[0].index;
			const hiddenItems = observedArray
				.filter(function(value) {
					return !value.visible && value.index < firstVisibleIndex;
				})
				.sort(function(a, b) {
					if (a.index < b.index) return -1;
					if (a.index > b.index) return 1;
					return 0;
				});

			if (hiddenItems.length < nextGroupLength) {
				// Only scroll the length of hidden items
				const firstHiddenItem = hiddenItems[0];
				items[firstHiddenItem.index].scrollIntoView({ behavior: 'smooth' });
				hiddenItemIndex = firstHiddenItem.index;
			} else {
				// Scroll the length of nextGroupLength
				// Only scroll the length of hidden items
				const firstHiddenItem =
					hiddenItems[firstVisibleIndex - nextGroupLength + 1];
				items[firstHiddenItem.index].scrollIntoView({ behavior: 'smooth' });
				hiddenItemIndex = firstHiddenItem.index;
			}
		}
	}

	// raf update functions
	function updatePaddles() {
		// If navItems scrollLeft is 0 don't show the left paddle
		// If navItems scrollLeft equals scrollWidth don't show the right paddle
		console.log('itemsWidth', itemsWidth, 'width', width);
		if (width < itemsWidth) {
			// Enable
			if (navItems.scrollLeft !== 0) {
				hideShowRafCall(false, startOfList);
				console.log('false, left');
			}
			if (navItems.scrollLeft !== navItems.scrollWidth) {
				hideShowRafCall(false, endOfList);
				console.log('false, right');
			}
		} else {
			// Disable left and right paddles
			hideShowRafCall(true, startOfList);
			console.log('true, left');
			hideShowRafCall(true, endOfList);
			console.log('true, right');
		}
	}
	// Add aria-hidden attributes to hide/show functions
	function hidePaddle(className, ticker) {
		const paddle = document.querySelector(className);
		paddle.disabled = true;
		paddle.setAttribute('aria-hidden', true);
		paddle.removeEventListener('click', handlePaddleClick);
		rafTickers[ticker] = false;
	}

	function showPaddle(className, ticker, direction) {
		const atStart = navItems.scrollLeft === 0 && direction === startOfList;
		const atEnd =
			navItems.scrollLeft === navItems.scrollWidth && direction === endOfList;

		if (width < itemsWidth && !atStart && !atEnd) {
			const paddle = document.querySelector(className);
			console.log('show paddle called');
			paddle.disabled = false;
			paddle.setAttribute('aria-hidden', false);
			// Removing event listener first to ensure there is only ever one
			paddle.removeEventListener('click', handlePaddleClick);
			paddle.addEventListener('click', handlePaddleClick);
		}
		rafTickers[ticker] = false;
	}

	// End raf update functions
	function setObserversOnItem(item) {
		const options = {
			root: navItems,
			rootMargin: '0px',
			threshold: 0.9
		};
		const observer = new IntersectionObserver(observerCallback, options);
		observer.observe(item);
	}

	function observerCallback(entries, observer) {
		entries.forEach(function(entry) {
			observedItems[entry.target.id].visible = entry.isIntersecting;
			if (observedItems[entry.target.id].index === 0 && entry.isIntersecting) {
				hideShowRafCall(true, startOfList);
			} else if (
				observedItems[entry.target.id].index === 0 &&
				!entry.isIntersecting
			) {
				hideShowRafCall(false, startOfList);
			} else if (
				observedItems[entry.target.id].index === numItemsObserved() - 1 &&
				entry.isIntersecting
			) {
				hideShowRafCall(true, endOfList);
			} else if (
				observedItems[entry.target.id].index === numItemsObserved() - 1 &&
				!entry.isIntersecting
			) {
				hideShowRafCall(false, endOfList);
			}
		});
	}

	function hideShowRafCall(hide, direction) {
		if (hide && direction === startOfList) {
			// Hide previous paddle button
			if (!rafTickers.hidePreviousPaddle) {
				requestAnimationFrame(function() {
					hidePaddle(leftPaddle, 'hidePreviousPaddle');
				});
				rafTickers.hidePreviousPaddle = true;
			}
		} else if (hide && direction === endOfList) {
			// Hide next paddle button
			if (!rafTickers.hideNextPaddle) {
				requestAnimationFrame(function() {
					hidePaddle(rightPaddle, 'hideNextPaddle');
				});
				rafTickers.hideNextPaddle = true;
			}
		} else if (!hide && direction === startOfList) {
			// Show previous paddle button
			if (!rafTickers.showPreviousPaddle) {
				requestAnimationFrame(function() {
					showPaddle(leftPaddle, 'showPreviousPaddle', startOfList);
				});
				rafTickers.showPreviousPaddle = true;
			}
		} else if (!hide && direction === endOfList) {
			// Show next paddle button
			if (!rafTickers.showNextPaddle) {
				requestAnimationFrame(function() {
					showPaddle(rightPaddle, 'showNextPaddle', endOfList);
				});
				rafTickers.showNextPaddle = true;
			}
		}
	}

	function numItemsVisible() {
		return Object.values(observedItems).filter(function(value) {
			return value.visible;
		}).length;
	}

	function numItemsObserved() {
		return items.length;
	}

	function debounce(callback, wait, immediate) {
		let timeout;
		return function() {
			const context = this;
			const args = arguments;
			const callbackForTimeout = function() {
				timeout = null;
				if (!immediate) {
					callback.apply(context, args);
				}
			};
			clearTimeout(timeout);
			timeout = setTimeout(callbackForTimeout, wait);
			if (immediate && !timeout) {
				callback.apply(context, args);
			}
		};
	}
}

document.getElementById('app').innerHTML = `
<div id="head">
  <h1>Responsive Menu</h1>
  <h4><a href="http://www.dbs.org">Designed for Digital Bible Society</a></h4>
</div>
<div>
  We use Parcel to bundle this sandbox, you can find more info about Parcel
  <a href="https://parceljs.org" target="_blank" rel="noopener noreferrer">here</a>.
</div>
`;