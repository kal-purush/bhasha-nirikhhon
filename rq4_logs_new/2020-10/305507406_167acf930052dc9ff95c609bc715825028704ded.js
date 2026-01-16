import React from 'react'
import s from './Button.module.css'

export const Button = ({ className, name, ...props }) => {
	const finalClassName = `${s.button} ${className}`
	return (
		<>
			<button className={finalClassName}>{name}</button>
		</>
	)
}