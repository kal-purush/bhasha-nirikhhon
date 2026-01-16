import React from 'react'
import './Portfolio.css'
import demo from './images/demo.jpg'

import SectionHeader from './SectionHeader'
import PortfolioItem from './PortfolioItem'

const projects = [
  {
    screenshot: demo,
    title: 'project1',
    types: ['web', 'mobile']
  },
  {
    screenshot: demo,
    title: 'project1',
    types: ['web', 'mobile']
  },
  {
    screenshot: demo,
    title: 'project1',
    types: ['web', 'mobile']
  },
  {
    screenshot: demo,
    title: 'project1',
    types: ['web', 'mobile']
  }
]

const Portfolio = () => {
  return (
    <article id="portfolio" className="Portfolio">
      <div className="Portfolio-banner">
        <div className="banner-mask" />
        <span className="divider" />
        <div className="banner-mask__white" />
        <SectionHeader
          className="Portfolio-header"
          title="portfolio"
          label="02."
        />
      </div>
      <ul className="Portfolio-list">
        {projects.map((item, index) => {
          return <PortfolioItem item={item} key={index} />
        })}
      </ul>
    </article>
  )
}

export default Portfolio