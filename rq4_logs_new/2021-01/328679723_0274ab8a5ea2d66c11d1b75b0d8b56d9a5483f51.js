export default function slider(item) {
  const sliderWrapper = item.querySelector('.slider-wrapper'),
      sliderItems = item.querySelectorAll('.slider-item'),
      sliderControlLeft = item.querySelector('.slider-control-left'),
      sliderControlRight = item.querySelector('.slider-control-right'),
      wrapperWidth = parseFloat(getComputedStyle(sliderWrapper).width),
      itemWidth = parseFloat(getComputedStyle(sliderItems[0]).width),
      step = itemWidth / wrapperWidth * 100,
      cards = item.querySelectorAll('.card-btn-open');

  let positionLeftItem = 0,
      translateX = 0,
      items = [],
      currentIndex = 1;

  sliderItems.forEach((el, i) => items.push({item: el, position: i, transform: 0}));

  cards.forEach((card, i) => card.addEventListener('click', () => setSlide(i + 1)));

  let position = {
    getItemMin() {
      let indexItem = 0;
      items.forEach((item, i) => {
        if (item.position < items[indexItem].position) {
          indexItem = i
        }
      });
      return indexItem
    },

    getItemMax() {
      let indexItem = 0;
      items.forEach((item, i) => {
        if (item.position > items[indexItem].position) {
          indexItem = i
        }
      })
      return indexItem
    },

    getMin() {
      return items[position.getItemMin()].position
    },

    getMax() {
      return items[position.getItemMax()].position
    }
  }

  function setSlide(newIndex) {
    if (newIndex === currentIndex) {
      return;
    }

    let direction;
    let count;
    if (newIndex > currentIndex) {
      direction = 'right';
      count = newIndex - currentIndex;
    } else if (newIndex < currentIndex) {
      direction = 'left';
      count = currentIndex - newIndex;
    }
    for (let i = 0; i < count; i++) {
      transformItem(direction);
    }
  }

  function transformItem(direction) {
    let nextItem;
    sliderWrapper.style.transition = 'transform 0.6s ease';
    if (direction === 'right') {
      positionLeftItem++;
      if (positionLeftItem + wrapperWidth / itemWidth - 1 >= position.getMax()) {
        nextItem = position.getItemMin();
        items[nextItem].position = position.getMax() + 1;
        items[nextItem].transform += items.length * 100;
        items[nextItem].item.style.transform = `translateX(${items[nextItem].transform}%)`;
      }
      translateX -= step;

      currentIndex++;
      if (currentIndex > items.length) {
        currentIndex = 1;
      }
    }
    if (direction === 'left') {
      positionLeftItem--;
      if (positionLeftItem <= position.getMin()) {
        nextItem = position.getItemMax();
        items[nextItem].position = position.getMin() - 1;
        items[nextItem].transform -= items.length * 100;
        items[nextItem].item.style.transform = 'translateX(' + items[nextItem].transform + '%)';
      }
      translateX += step;

      currentIndex--;
      if (currentIndex === 0) {
        currentIndex = items.length;
      }
    }
    sliderWrapper.style.transform = `translateX(${translateX}%)`;
  }

  sliderControlLeft.addEventListener('click', () => transformItem('left'));
  sliderControlRight.addEventListener('click', () => transformItem('right'));
}