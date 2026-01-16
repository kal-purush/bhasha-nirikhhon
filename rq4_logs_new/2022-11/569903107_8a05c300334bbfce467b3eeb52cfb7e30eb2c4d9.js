const navBtn = document.querySelector(".hamburger")
const navMobile = document.querySelector(".header__nav-mobile-box")
const hamburgerBar = document.querySelector(".hamburger-inner")
const hamburgerBarAfter = document.querySelector(".hamburger-inner::after")
const hamburgerBarBefore = document.querySelector(".hamburger-inner::before")
const navLinks = document.querySelectorAll(".link-js")
const webBody = document.querySelector("body")
const footerYear = document.querySelector(".footer__year")
const allSections = document.querySelectorAll(".section")

const handleNav = () => {
	navLinks.forEach(item => {
		item.addEventListener("click", () => {
			navMobile.classList.remove("header__nav-mobile-box--active")
			navBtn.classList.remove("is-active")
			hamburgerBar.classList.remove("hamburger-fixed")
			hamburgerBarAfter.classList.remove("hamburger-fixed")
			hamburgerBarBefore.classList.remove("hamburger-fixed")
			webBody.style.overflowY = "scroll"
		})
	})

	navBtn.classList.toggle("is-active")
	navMobile.classList.toggle("header__nav-mobile-box--active")

	if (navBtn.classList.contains("is-active")) {
		hamburgerBar.classList.add("hamburger-fixed")
		hamburgerBarAfter.classList.add("hamburger-fixed")
		hamburgerBarBefore.classList.add("hamburger-fixed")
		webBody.style.overflowY = "hidden"
	} else {
		hamburgerBar.classList.remove("hamburger-fixed")
		webBody.style.overflowY = "scroll"
	}
}

navBtn.addEventListener("click", handleNav)

const handleCurrentYear = () => {
	const year = new Date().getFullYear()
	footerYear.innerText = year
}

handleCurrentYear()

const handleObserver = () => {
	const currentSection = window.scrollY

	allSections.forEach(section => {
		if (
			section.classList.contains("white-section") &&
			section.offsetTop <= currentSection + 60
		) {
			hamburgerBar.classList.add("hamburger-fixed")
			hamburgerBarAfter.classList.add("hamburger-fixed")
			hamburgerBarBefore.classList.add("hamburger-fixed")
		} else if (
			!section.classList.contains("white-section") &&
			section.offsetTop <= currentSection + 60
		) {
			hamburgerBar.classList.remove("hamburger-fixed")
			hamburgerBarAfter.classList.remove("hamburger-fixed")
			hamburgerBarBefore.classList.remove("hamburger-fixed")
		}
	})
}

window.addEventListener("scroll", handleObserver)