import Link from 'next/link'
import Head from 'next/head'

export default ({ children, id, className }) => (
  <div className={`content-container ${className}`} id={id}>
    <div className="sleeve">
    { children }
    </div>
  </div>
)