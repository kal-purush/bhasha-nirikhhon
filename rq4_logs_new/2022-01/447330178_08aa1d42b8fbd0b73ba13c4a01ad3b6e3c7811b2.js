import * as React from 'react'
import { useState } from 'react'
import BubbleChart from '@weknow/react-bubble-chart-d3';
import { SiteIcons } from './site-icons';

const LangChart = ({ repos, blurbs }) => {
    const totalRepos = repos.nodes.length;

    var totalSize = 0
    const allLanguagesData = {}
    repos.nodes.forEach(repo => {
      repo.languages.edges.forEach(language => {
        allLanguagesData[language.node.name] = allLanguagesData[language.node.name] || {label: '', value: 0, color: ''}
        totalSize += language.size
        allLanguagesData[language.node.name].label = language.node.name
        allLanguagesData[language.node.name].value += language.size
        allLanguagesData[language.node.name].color = language.node.color
  
      })
    })

    const data = Object.values(allLanguagesData).sort((a, b) => b.value - a.value).slice(0, 8)
    data.forEach(language => language.color += 'e6')

    const [blurb, setBlurb] = useState('')
    const langClick = (label) => setBlurb(blurbs.find(blurb => blurb.name === label.toLowerCase()).blurb || '')

    var width = 375;

    return (
        <div style={{ textAlign: 'center'}}>
            <h2 style={{marginBottom: '5px'}}>My 8 Most Used Languages</h2>
            <div style={{ fontStyle: 'italic', fontSize: '14px', marginBottom: '15px'}}>
                According to my GitHub&nbsp;
                <div className='tooltip' style={{ display: 'inline'}}>
                    <span className='tooltiptext tooltiptop'>{`Queried ${totalRepos} repositories`}</span>
                    <SiteIcons.FaInfoCircle/>
                </div>
            </div>
            <BubbleChart
                graph= {{
                    zoom: 1.1,
                    offsetX: -0.08,
                    offsetY: -0.01,
                }}
                width={width}
                height={width+50}
                padding={0} // optional value, number that set the padding between bubbles
                showLegend={false} // optional value, pass false to disable the legend.
                valueFont={{
                    size: 0,
                    color: '#fff',
                    }}
                labelFont={{
                    size: 16,
                    color: '#000',
                    // weight: '400',
                    strokeWidth: '1px'
                }}
                bubbleClickFun={langClick}
                data={data}
            ></BubbleChart>
            <p style={{ minHeight: '20px', margin: '0 20px' }}>{blurb}</p>
        </div>
    )
}

export default LangChart