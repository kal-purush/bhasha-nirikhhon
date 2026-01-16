async function displayArchitectWorks() {
    const response = await fetch('http://localhost:5678/api/works');
    const works = await response.json();
    console.log("Architect's Works:", works);

    const galleryContainer = document.querySelector('.gallery');

    galleryContainer.innerHTML = '';
  
    works.forEach(work => {
      const figure = document.createElement('figure');
      const img = document.createElement('img');
      const figcaption = document.createElement('figcaption');
  
      img.src = work.imageUrl;
      img.alt = work.title;
      figcaption.textContent = work.title;
  
      figure.appendChild(img);
      figure.appendChild(figcaption);
  
      galleryContainer.appendChild(figure);
    });
  
    console.log("Architect's Works:", works);
  }

  displayArchitectWorks();
  