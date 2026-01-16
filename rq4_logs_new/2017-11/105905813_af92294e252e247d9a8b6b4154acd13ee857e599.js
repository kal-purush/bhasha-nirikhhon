import React from 'react';

export default ({
  sectionId,
  target,
  children,
  className,
  id,
  style,
}) => (
  <a href={`#${sectionId}`} target={target} className={className} id={id} style={style}>
    {children}*
  </a>
)