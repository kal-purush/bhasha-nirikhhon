import React from 'react'

export const Transaction = ({ text, amount }) => {
  const sign = amount < 0 ? '-' : '+'

  return (
    <li
      className={
        'transaction__item ' + (amount < 0 ? 'transaction__item--plus' : 'transaction__item--minus')
      }>
      {text}
      <span>
        {sign}
        {Math.abs(amount)} 원
      </span>
      <button className="btn--delete">x</button>
    </li>
  )
}