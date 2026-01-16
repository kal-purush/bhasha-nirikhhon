const setupNavClick = (): void => {
  const beanies: Element | null = document.querySelector('.beanies-nav');
  const facemasks: Element | null = document.querySelector('.facemasks-nav');
  const gloves: Element | null = document.querySelector('.gloves-nav');
  const beaniesLink: Element | null = document.querySelector('.beanies-link');
  const facemasksLink: Element | null = document.querySelector('.facemasks-link');
  const glovesLink: Element | null = document.querySelector('.gloves-link');

  const handleNavClick = e => {
    // Clicked product should be only active product in nav
    if (glovesLink !== null && beaniesLink !== null && facemasksLink !== null ) {
      if (beaniesLink === e.target) {
        glovesLink!.classList.remove('active');
        facemasksLink!.classList.remove('active');
      } else if (facemasksLink === e.target) {
        beaniesLink!.classList.remove('active');
        glovesLink!.classList.remove('active');
      } else if (glovesLink === e.target) {
        beaniesLink!.classList.remove('active');
        facemasksLink!.classList.remove('active');
      }
    }
    e.target.classList.add("active");
  }
  beanies?.addEventListener("click", handleNavClick);
  facemasks?.addEventListener("click", handleNavClick);
  gloves?.addEventListener("click", handleNavClick);
}

const selectedProduct = (slug): void => {
  // Select the nav product in view at load
  if (slug === 'beanies') {
    const beaniesLink: Element | null = document.querySelector('.beanies-link');
    if (beaniesLink !== null) {
      beaniesLink!.classList.add('active');
    }
  } else if (slug === 'facemasks') {
    const facemasksLink: Element | null = document.querySelector('.facemasks-link');
    if (facemasksLink !== null) {
      facemasksLink!.classList.add('active');
    }
  } else if (slug === 'gloves') {
    const glovesLink: Element | null = document.querySelector('.gloves-link');
    if (glovesLink !== null) {
      glovesLink!.classList.add('active');
    }
  }
}

export { setupNavClick, selectedProduct };