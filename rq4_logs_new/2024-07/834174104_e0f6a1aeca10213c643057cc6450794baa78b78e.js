import React, { useEffect, useState } from 'react';

const MentorPage = () => {
  const [selectedCard, setSelectedCard] = useState(null);

  useEffect(() => {
    const filter = document.querySelector('#fill');
    filter.addEventListener('click', () => {
      const filterClass = document.querySelector('.filter-section');
      filterClass.style.display = "block";
    });

    const mentorCards = document.querySelectorAll('.mentor-card');
    const mentorProfile = document.querySelector('.mentor-profile');

    mentorCards.forEach(card => {
      card.addEventListener('click', () => {
        mentorCards.forEach(c => c.classList.remove('selected'));
        card.classList.add('selected');
        setSelectedCard(card);
        updateMentorProfile(card);
      });
    });

    return () => {
      mentorCards.forEach(card => {
        card.removeEventListener('click', () => {});
      });
    };
  }, []);

  const updateMentorProfile = (card) => {
    const name = card.querySelector('h3').textContent;
    const experience = card.querySelector('p:nth-of-type(1)').textContent;
    const expertise = card.querySelector('p:nth-of-type(2)').textContent;
    const imageUrl = card.querySelector('img').src;

    return (
      <div className="mentor-profile">
        <h2>Mentor Profile</h2>
        <img src={imageUrl} alt={name} />
        <h3>{name}</h3>
        <p>{expertise}</p>
        <p>{experience}</p>
        
        <h4>About Mentor</h4>
        <p>Detailed information about {name} would go here.</p>
  
        <h4>Education</h4>
        <ul>
          <li>Placeholder education information</li>
        </ul>
  
        <h4>Work Experience</h4>
        <ul>
          <li>Placeholder work experience information</li>
        </ul>
  
        <h4>Reviews</h4>
        <div className="review">
          <p>"Placeholder review for {name}" - Anonymous</p>
        </div>
  
        <button className="book-session">Book a Session</button>
      </div>
    );
  };

  return (
    <div>
      {/* Your existing JSX code */}
      {selectedCard && updateMentorProfile(selectedCard)}
    </div>
  );
};

export default MentorPage;