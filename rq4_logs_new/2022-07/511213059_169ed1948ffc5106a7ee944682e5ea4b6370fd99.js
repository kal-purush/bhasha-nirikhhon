/* =========================================================================
 * FUNCTIONS
 * ========================================================================= */

/* SLIDER ITEMS
 * ------------------------------------------------------------------------- */

function goToNextSliderItem($activeSliderItem, $nextSliderItem) {
  $activeSliderItem.removeClass("slider__item--active");
  $nextSliderItem.addClass("slider__item--active");

  const $slider = $activeSliderItem.parents(".slider");
  updateBullets($slider);
}

/* BULLETS
 * ------------------------------------------------------------------------- */

function createBullets($slider) {
  const $bulletContainer = $slider.children(".slider__bullet-container");
  const sliderItemCount = $slider.find(".slider__item").length;
  const activeSliderItemIndex = $slider.find(".slider__item--active").index();

  for (let i = 0; i < sliderItemCount; i++) {
    const $newBulletHtmlNode = $("<li>").addClass("slider__bullet");
    if (i == activeSliderItemIndex)
      $newBulletHtmlNode.addClass("slider__bullet--active");
    $bulletContainer.append($newBulletHtmlNode);
  }
}

function updateBullets($slider) {
  $slider.find(".slider__bullet--active").removeClass("slider__bullet--active");
  const activeSliderItemIndex =
    $slider.find(".slider__item--active").index() + 1;

  $slider
    .find(".slider__bullet:nth-child(" + activeSliderItemIndex + ")")
    .addClass("slider__bullet--active");
}

/* =========================================================================
 * EVENTS
 * ========================================================================= */

/* SLIDER ITEMS
 * ------------------------------------------------------------------------- */

$(".slider__button").on("click", function () {
  const $activeSliderItem = $(this)
    .siblings(".slider__item-list")
    .children(".slider__item--active");

  let $nextSliderItem =
    $activeSliderItem.next(".slider__item").length != 0
      ? $activeSliderItem.next(".slider__item")
      : $(".slider__item:first-child");

  if ($(this).hasClass("slider__button--previous")) {
    $nextSliderItem =
      $activeSliderItem.prev(".slider__item").length != 0
        ? $activeSliderItem.prev(".slider__item")
        : $(".slider__item:last-child");
  }

  goToNextSliderItem($activeSliderItem, $nextSliderItem);
});

/* BULLETS
 * ------------------------------------------------------------------------- */

$(document).on("click", ".slider__bullet", function () {
  if ($(this).hasClass("slider__bullet--active")) {
    return;
  }

  const $activeSliderItem = $(this)
    .parents(".slider")
    .find(".slider__item--active");

  const nextSliderItemIndex = $(this).index();
  const $nextSliderItem = $(
    ".slider__item:nth-child(" + (nextSliderItemIndex + 1) + ")"
  );

  goToNextSliderItem($activeSliderItem, $nextSliderItem);
});

/* =========================================================================
 * PROGRAM
 * ========================================================================= */

$(document).ready(function () {
  $(".slider").each(function () {
    createBullets($(this));
  });
});