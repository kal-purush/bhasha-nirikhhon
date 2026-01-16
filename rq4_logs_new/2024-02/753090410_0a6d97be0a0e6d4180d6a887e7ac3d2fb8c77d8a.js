let vh = window.innerHeight * 0.01;
document.documentElement.style.setProperty('--vh', `${vh}px`);
const locationIcon = document.getElementById('doGeolocation')
locationIcon.addEventListener('click', () => {
    locationIcon.classList.add('selected')
});
function bodyZoom() {
    if (window.devicePixelRatio > 1) {
        jQuery('body').addClass('zoom150');
    } else {
        jQuery('body').removeClass('zoom150');
    }
}
function initLocator() {
    geocoder = new google.maps.Geocoder();
    autocomplete = new google.maps.places.Autocomplete(document.getElementById('locator_address'), {
        types: ['geocode'],
        fields: ['name', 'geometry.location'],
        componentRestrictions: {
            country: bandidos20.googlemaps_country
        }
    });
    autocomplete.addListener('place_changed', function () {
        var place = autocomplete.getPlace();
        if (place.geometry) {
            jQuery('input[name="lat"]').val(place.geometry.location.lat());
            jQuery('input[name="lng"]').val(place.geometry.location.lng());
            jQuery('#doGeolocation').addClass('selected');
            jQuery('[data-toggle="datepicker"]').datepicker('show');
        }
    });
    jQuery("#locator_address").focusin(function () {
        jQuery("#locator_address").keypress(function (e) {
            jQuery('.address_wrap').removeClass('geolocated');
            jQuery('#doGeolocation').removeClass('selected');
            if (e.which == 13) {
                e.preventDefault();
                var firstResult = jQuery(".pac-container .pac-item:first .pac-item-query").text();
                var geocoder = new google.maps.Geocoder();
                geocoder.geocode({ "address": firstResult }, function (results, status) {
                    if (status == google.maps.GeocoderStatus.OK) {
                        console.log(results[0].address_components[0]);
                        var lat = results[0].geometry.location.lat(),
                            lng = results[0].geometry.location.lng(),
                            placeName = results[0].address_components[0].short_name,
                            latlng = new google.maps.LatLng(lat, lng);
                        jQuery(".pac-container .pac-item:first").addClass("pac-selected");
                        jQuery(".pac-container").css("display", "none");
                        jQuery("#locator_address").val(firstResult);
                        jQuery(".pac-container").css("visibility", "hidden");
                        jQuery('input[name="lat"]').val(lat);
                        jQuery('input[name="lng"]').val(lng);
                        jQuery('#doGeolocation').addClass('selected');
                        jQuery('[data-toggle="datepicker"]').datepicker('show');
                    }
                });
            } else {
                jQuery(".pac-container").css("visibility", "visible");
            }
        });
    });
    if (jQuery('.locationContainer').length > 0) {
        map = new google.maps.Map(document.getElementById('mapContainer'), {
            center: { lat: 40.436775, lng: -3.69379 },
            zoom: 7,
            disableDefaultUI: true,
        });
        var restaurantPosition = { lat: jQuery('.locationContainer').data('lat'), lng: jQuery('.locationContainer').data('lng') };
        map.setCenter(restaurantPosition);
        map.setZoom(15);
        marker = new google.maps.Marker({
            map: map,
            position: restaurantPosition,
            icon: new google.maps.MarkerImage('/wp-content/themes/bandidosgrill17/library/images/map-marker.png',
                null, null, null, new google.maps.Size(45, 50))
        });
        marker.setOpacity(1);
        var panAmount = jQuery('#container').width() * 0.33;
        map.panBy(-panAmount, -30);
    }
}

if (document.readyState === "complete" || (document.readyState !== "loading" && !document.documentElement.doScroll)) {
    doLazyLoad();
} else {
    document.addEventListener('DOMContentLoaded', doLazyLoad);
}
function doLazyLoad() {
    var isIE11 = !!window.MSInputMethodContext && !!document.documentMode;
    if (!isIE11) {
        var images = _toConsumableArray(document.querySelectorAll('.lazy-image'));
        var interactSettings = {
            root: document.querySelector('.center'),
            rootMargin: '0px 0px 200px 0px'
        };
        function onIntersection(imageEntites) {
            imageEntites.forEach(function (image) {
                if (image.isIntersecting) {
                    observer.unobserve(image.target);
                    image.target.src = image.target.dataset.src;
                    if (image.target.dataset.srcset) {
                        image.target.srcset = image.target.dataset.srcset;
                    }
                    if (image.target.dataset.sizes) {
                        image.target.sizes = image.target.dataset.sizes;
                    }
                    image.target.onload = function () {
                        return image.target.classList.add('loaded');
                    };
                }
            });
        }
        var observer = new IntersectionObserver(onIntersection, interactSettings);
        images.forEach(function (image) {
            return observer.observe(image);
        });
    } else {
        var images = document.querySelectorAll('.lazy-image');
        for (var i = 0; i < images.length; i++) {
            images[i].setAttribute("src", images[i].getAttribute('data-src'));
            if (images[i].getAttribute('data-srcset')) {
                images[i].setAttribute("srcset", images[i].getAttribute('data-srcset'));
            }
            if (images[i].getAttribute('data-sizes')) {
                images[i].setAttribute("sizes", images[i].getAttribute('data-sizes'));
            }
            images[i].onload = function () {
                return images[i].classList.add('loaded');
            };
        }
    }
}
function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _nonIterableSpread(); }
function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance"); }
function _iterableToArray(iter) { if (Symbol.iterator in Object(iter) || Object.prototype.toString.call(iter) === "[object Arguments]") return Array.from(iter); }
function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = new Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } }
var closeModal = document.querySelector('.show__close') !== null;
var modal = document.querySelector('.promo_modal_bg');
if (closeModal) {
    closeModal.addEventListener("click", function (e) {
        e.preventDefault();
        modal.style.display = 'none';
    });
}
window.onclick = function (event) {
    if (event.target == modal) {
        modal.style.display = "none";
    }
}
function setCookie(cname, cvalue, exdays) {
    const d = new Date();
    d.setTime(d.getTime() + (exdays * 24 * 60 * 60 * 1000));
    let expires = "expires=" + d.toUTCString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
}
function getCookie(cname) {
    let name = cname + "=";
    let ca = document.cookie.split(';');
    for (let i = 0; i < ca.length; i++) {
        let c = ca[i];
        while (c.charAt(0) == ' ') {
            c = c.substring(1);
        }
        if (c.indexOf(name) == 0) {
            return c.substring(name.length, c.length);
        }
    }
    return "";
}
const dropdownMenu = document.getElementById('dropdown-menu');
const openDropdownBtnMobile = document.getElementById('menu-navtrigger-mobile');
const openDropdownBtnDesktop = document.getElementById('menu-navtrigger-desktop');
const openDropdownBtnDesktopSticky = document.getElementById('menu-navtrigger-sticky');
const reserveBar = document.querySelector('.fast__booking');
const reserveBtn = document.getElementById('reserve-btn');
const reserveBtnWrapper = document.getElementById('reserve-btn-wrapper');
const wrapper = document.querySelector('.fast__back');
const languageBtnSticky = document.getElementById('language-btn-desktop-sticky');
const languageBtnMenu = document.getElementById('language-btn-menu');
const closeDropdownBtn = document.getElementById('header-dropdown-close');
const line1 = document.getElementById('line1');
const line2 = document.getElementById('line2');
const languageSwitch = document.getElementById('language-switch');
const languageSwitchSticky = document.getElementById('language-switch-sticky');
const languageSwitchMenu = document.getElementById('language-switch-menu');
let openedOptions = false;
const arrowDown = document.querySelector('.arrow-drop-down');
const arrowDownMenu = document.getElementById('arrow-drop-down-menu');
const arrowDownSticky = document.getElementById('arrow-drop-down-sticky');
const reserveDateLabel = document.getElementById('reserve-date-label');
const reserveDateInput = document.getElementById('reserve-date-input');
const footer = document.getElementById('bandidos_footer');
const topButton = document.querySelector('.scroll__top')
const socialNetwork = document.getElementById('social-network-container')
const primaryMenu = document.getElementById('primary-menu')
const logoRotate = document.getElementById('logo-dropdown')
function visibleFooter() {
  
    const rect = footer.getBoundingClientRect();
    return rect.top <= (window.innerHeight || document.documentElement.clientHeight);
}
function openDropdown(element) {
    dropdownMenu.classList.remove('hidden-dropdown');
    dropdownMenu.classList.add('dropdown-menu');
    dropdownMenu.classList.add('show-menu');
    dropdownMenu.classList.remove('close-menu');
    closeDropdownBtn.addEventListener('click', () => {
        dropdownMenu.classList.remove('add');
        dropdownMenu.classList.remove('show-menu');
        dropdownMenu.classList.add('close-menu');
        setTimeout(() => {
            line1.classList.remove('animate-1');
            line2.classList.remove('animate-2');
            socialNetwork.classList.remove('animate-social-network')
            primaryMenu.classList.remove('animate-primary-menu')
            logoRotate.classList.remove('animate-logo-rotate')
        }, 500);
    });
}
openDropdownBtnDesktop.addEventListener('click', () => {
    openDropdown();
    line1.classList.add('animate-1')
    line2.classList.add('animate-2')
    socialNetwork.classList.add('animate-social-network')
    primaryMenu.classList.add('animate-primary-menu')
    logoRotate.classList.add('animate-logo-rotate')
})
openDropdownBtnMobile.addEventListener('click', () => {
    openDropdown();
    line1.classList.add('animate-1')
    line2.classList.add('animate-2')
    socialNetwork.classList.add('animate-social-network')
    primaryMenu.classList.add('animate-primary-menu')
    logoRotate.classList.add('animate-logo-rotate')
})
openDropdownBtnDesktopSticky.addEventListener('click', () => {
    openDropdown();
    line1.classList.add('animate-1')
    line2.classList.add('animate-2')
    socialNetwork.classList.add('animate-social-network')
    primaryMenu.classList.add('animate-primary-menu')
    logoRotate.classList.add('animate-logo-rotate')
})
if (null !== reserveBtn) {
    reserveBtn.addEventListener('click', () => {
        reserveBar.classList.remove('bar-hidden');
        reserveBtn.classList.remove('reserve-btn-visible');
        reserveBar.classList.add('is--visible');
        reserveBtn.classList.add('reserve-btn-hidden');
        wrapper.classList.add('visible');
    });
}
wrapper.addEventListener('click', () => {
    reserveBar.classList.add('bar-hidden');
    reserveBar.classList.remove('is--visible');
    wrapper.classList.remove('visible');
    if (!visibleFooter()) {
        reserveBtnWrapper.style.opacity = '0';
        reserveBtn.classList.remove('reserve-btn-hidden');
        reserveBtn.classList.remove('hidden');
        reserveBtn.classList.add('reserve-btn-visible');
        setTimeout(() => {
            reserveBtn.classList.remove('reserve-btn-visible');
            reserveBtnWrapper.style.opacity = '1';
        }, 700);
    }
});
function handleArrows(arrow) {
    if (arrow.classList.contains('active')) {
        arrow.classList.remove('active');
    } else {
        arrow.classList.add('active');
    }
}
function openLanguageOptions(element, arrow) {
    if (!element.classList.contains('open')) {
        element.classList.add('open');
        handleArrows(arrow);
    } else {
        element.classList.remove('open');
        handleArrows(arrow);
    }
}
languageBtnMenu.addEventListener('click', () => {
    openLanguageOptions(languageSwitchMenu, arrowDownMenu)
})
languageBtnSticky.addEventListener('click', () => {
    openLanguageOptions(languageSwitchSticky, arrowDownSticky)
})
reserveDateLabel.addEventListener('click', () => {
    reserveDateInput.classList.remove('hidden');
    reserveDateLabel.classList.add('hidden');
});
function handleFooterVisibility() {
    if (reserveBar.classList.contains('bar-hidden')) {
        if (visibleFooter()) {
            reserveBtn.classList.add('reserve-btn-hidden');
        } else {
            if (reserveBtn.classList.contains('reserve-btn-hidden') && !reserveBtn.classList.contains('reserve-btn-visible')) {
                reserveBtnWrapper.style.opacity = '0';
                reserveBtn.classList.remove('reserve-btn-hidden');
                reserveBtn.classList.add('reserve-btn-visible');
                setTimeout(() => {
                    reserveBtn.classList.remove('reserve-btn-visible');
                    reserveBtnWrapper.style.opacity = '1';
                }, 700);
            };
            reserveBtn.classList.remove('hidden');
        };
    }
}
window.addEventListener('scroll', handleFooterVisibility);
handleFooterVisibility();
topButton.addEventListener('click', () => {
    window.scrollTo({
        top: 0,
        behavior: 'smooth'
    });
});
const header = document.getElementById('header');
const stickyHeader = document.getElementById('sticky-header');
let lastScrollTop = 0;
let opacity = 1;
window.addEventListener('scroll', () => {
    let st = window.scrollY || document.documentElement.scrollTop;
    if (window.scrollY < 900) {
        stickyHeader.classList.remove('sticky-visible');
        stickyHeader.style.opacity = 0;
        languageSwitchSticky.classList.remove('open');
    }
    if (opacity <= 0) {
        header.classList.add('hidden-header');
    }
    if (window.scrollY < 700) {
        header.classList.remove('hidden');
        if (window.scrollY === 0) {
            header.style.opacity = 1;
        } else {
            if (st > lastScrollTop && opacity > 0) {
                opacity -= 0.01;
            }
            if (st < lastScrollTop && opacity < 1) {
                opacity += 0.01;
            }
            header.style.opacity = opacity;
        }
        header.classList.remove('hidden-header');
        arrowDownSticky.classList.remove('active');
    } else {
        header.style.opacity = 0;
        header.classList.add('hidden');
        if (window.scrollY > 900 && st < lastScrollTop) {
            stickyHeader.classList.add('sticky-visible');
            stickyHeader.style.opacity = 1;
        } else if (window.scrollY > 700 && st > lastScrollTop) {
            stickyHeader.classList.remove('sticky-visible');
        }
    }
    lastScrollTop = st <= 0 ? 0 : st;
});