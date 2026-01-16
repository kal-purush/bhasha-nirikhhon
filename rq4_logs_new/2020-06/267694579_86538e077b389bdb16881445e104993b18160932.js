
import React from 'react'

export default function NavCircleButton(props) {
    const { tag, className, childrenm, ...otherProps } = props

    return React.createElement(
        props.tag,
        {
            className: ['NavCircleButton', props.className].join(' '),
            ...otherProps
        },
        props.children
    )
}

NavCircleButton.defaultProps = {
    tag: 'a',
}