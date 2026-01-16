import React, { lazy, useEffect } from 'react'
import './App.css'
import About from './components/About'
import Projects from './components/Projects/Projects'
// import MoreProjects from './components/MoreProjects/MoreProjects'
import Contact from './components/Contact'
import { Element } from 'react-scroll'
import ReactGa from 'react-ga'
import styled from 'styled-components'

const Container = styled.div`
  width: 1200px;
  max-width: 1200px;
  box-shadow: 0px 0px 5px 3px rgba(0, 0, 0, 0.2);
  background: white;
`
const Header = lazy(() => import('./components/Header/index'))

function Home() {
  useEffect(() => {
    ReactGa.initialize('UA-145732904-1')
    ReactGa.pageview('/')
  }, [])

  return (
    <Container>
      <Header />
      <Element
        name='about'
        className='element'
        style={{ position: 'relative' }}
      >
        <About />
      </Element>
      <Element name='projects' className='element'>
        <Projects />
        {/* <MoreProjects /> */}
      </Element>
      <Element name='contact' className='element'>
        <Contact />
      </Element>
    </Container>
  )
}

export default Home