import React from 'react'

const Article = (props) => {
    return (
        <a className={'article ' + 'article__' + props.articleName} href={props.url}>
            <section className='article__text-holder'>
                <h2 className='article__header'>{props.header}</h2>
            </section>
        </a>
    )
}

export default Article