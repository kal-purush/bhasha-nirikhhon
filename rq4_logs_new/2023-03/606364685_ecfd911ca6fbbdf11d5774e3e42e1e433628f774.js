import React from 'react'
import { Card } from 'antd'
import Navbar from '../components/Navbar'
import './styles/KinestheticLearning.css'
function KinestheticLearning() {
  return (
    <>
      <Navbar />
      <div className='kinesthetic-container' >
        <h1 className='kinesthetic-title'>Kinesthetic Learning Approach</h1>
        <p className='kinesthetic-subtitle'>How to deal with kinesthetic learners?</p>
        <img className='kinesthetic-head-img' src='https://pixy.org/src/31/thumbs350/310680.jpg' />

        <p style={{ fontSize: '1.2rem', fontWeight: 'bold', textAlign: 'justify', lineHeight: '1.6rem' }}>
          kinesthetic who is prefer learning by their hand, and practicing, kinesthetic learners process information best when they are physically engaged during the learning process.
        </p>
        <p style={{ fontSize: '1.1rem', color: '#4e5250', fontWeight: 'bold', textAlign: 'justify', lineHeight: '1.6rem' }}>
          kinesthetic learning style people have trouble with learning through traditional lecture-based learning because the body fails to recognize the connection when they're listening without movement. Although their bodies are not actively involved, their minds are, which makes it more challenging for them to digest the information. They frequently need to stand up and move around in order to commit information to memory        </p>
        <iframe width="100%" height="520" src="https://www.youtube.com/embed/Om6pklgh3Ws" title="STEPS: Kinesthetic Learners!" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>



        <h3 style={{ fontSize: '1.5rem', marginTop: '35px', marginBottom: '10px' }}>Advantages of Kinesthetic Learning</h3>
        <ul className='kinesthetic-advantages-list'>
          <li className='kinesthetic-advantages-item'>
            the excellent synergy between the hands and the eyes
          </li>
          <li className='kinesthetic-advantages-item'>
            quick responses
          </li>
          <li className='kinesthetic-advantages-item'>
            superior motor memory (can duplicate something after doing it once)
          </li>
          <li className='kinesthetic-advantages-item'>
            brilliant experimenters
          </li>
          <li className='kinesthetic-advantages-item'>
            Good in sports and art and drama
          </li>
          <li className='kinesthetic-advantages-item'>
            High energy levels
          </li>
        </ul>


        <h3 style={{ fontSize: '1.5rem', marginTop: '35px', marginBottom: '15px' }}>Strategies to use with kinesthetic learners</h3>
        <div style={{ marginLeft: '25px' }}>
          {/* <h3 style={{ marginBottom: '10px' }}>1. Text-to-speech software</h3> */}
          <p className='kinesthetic-paragraph'>
            <strong>1. </strong> <strong style={{ color: '#353535' }}>Stand Up Instead of Sitting Down </strong>
            You are already aware of the negative effects prolonged sitting has on your health. However, did you know that standing up would enhance your knowledge and recall as a kinesthetic learner? Your body is more attentive and involved in the learning process when you are standing. A standing desk or book stand may enable you to focus for longer periods of time and retain more of what you read.
          </p>

          <p className='kinesthetic-paragraph'>
            <strong>2. </strong> Combine Your Study Session with Exercise:
            Instead of plopping on the sofa with your notes, get up and do burpees or jumping jacks in between chapters. Quiz the students while they shoot hoops or jump rope. Combining activities keep them energized and cements the ideas they study in their brain.
          </p>

          <p className='kinesthetic-paragraph'>
            <strong>3. </strong>
            Allow kinesthetic learners to stand, bounce their legs, or doodle during lectures. You will get more out of them in class if they can move around a little bit.
          </p>
          <iframe width="100%" height="520" src="https://www.youtube.com/embed/Q_PYBuIy8UA" title="Interactive Activities in Workshops and Presentations" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
          <p className='kinesthetic-paragraph'>
            <strong>4. </strong>
            Offer various methods of instruction—lectures, paired readings, group work, experiments, projects, plays, etc
          </p>
          <p className='kinesthetic-paragraph'>
            <strong>5. </strong>
            Ask your kinesthetic learners to complete relevant tasks during the lecture, like filling out a worksheet or taking notes.
          </p>
          <p className='kinesthetic-paragraph'>
            <strong>6. </strong>
            Allow kinesthetic learners to perform movement tasks before and after lectures, like handing out quizzes, writing on the chalkboard, or even rearranging desks.
          </p>
          <p className='kinesthetic-paragraph'>
            <strong>7. </strong>
            If you feel the kinesthetic learners slipping away from you in class, pause the lecture and have the whole class do something energetic: marching, stretching, or switching desks.
          </p>
          <p className='kinesthetic-paragraph'>
            <strong>8. </strong>
            Keep your lectures short and sweet! Plan several different activities throughout each class period in order to be mindful of all your students' learning styles.
          </p>
          <p className='kinesthetic-paragraph'>
            <strong>9. </strong>
            Use of plastic or magnetic letters for spelling, as Use a cookie sheet with magnetic letters to practice spelling words with children.
          </p>
          <p className='kinesthetic-paragraph'>
            <strong>10. </strong>
            Use of games and projects to learn new concepts.
          </p>
          <p className='kinesthetic-paragraph'>
            <strong>11. </strong>
            Use of manipulatives and 3D models.
          </p>
          <p className='kinesthetic-paragraph'>
            <strong>12. </strong>
            Use of FlashCards,
          </p>
          <iframe width="100%" height="520" src="https://www.youtube.com/embed/wTfA5Bm_rN8" title="Kinesthetic Teaching" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
        </div>
      </div>
    </>)
}

export default KinestheticLearning