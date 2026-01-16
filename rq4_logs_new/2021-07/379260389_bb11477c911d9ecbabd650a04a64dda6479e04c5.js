import { ConstructorElement, DragIcon } from '@ya.praktikum/react-developer-burger-ui-components';
import React, { useRef } from 'react';
import { useDrag, useDrop } from 'react-dnd';

import styles from './DraggableIngredient.module.css';


export const DraggableIngredient = ({ uniqId, index, name, price, image, onHandleClose }) => {

  const ref = useRef(null);

  const [, drop] = useDrop({
    accept: 'currentIngredient',

    hover(item, monitor) {

      if (!ref.current) {
        return;
      }
      const dragIndex = item.index;
      const hoverIndex = index;
      if (dragIndex === hoverIndex) {
        return;
      }
      const hoverBoundingRect = ref.current.getBoundingClientRect();
      const hoverMiddleY = (hoverBoundingRect.bottom - hoverBoundingRect.top) / 2;
      const clientOffset = monitor.getClientOffset();
      const hoverClientY = clientOffset.y - hoverBoundingRect.top;

      if (dragIndex < hoverIndex && hoverClientY < hoverMiddleY) {
        return;
      }
      if (dragIndex > hoverIndex && hoverClientY > hoverMiddleY) {
        return;
      }
      item.index = hoverIndex;
    },
  });

  const [{ isDrag }, dragRef] = useDrag({
    type: 'currentIngredient',
    item: { id: uniqId, index: index, type: 'currentIngredient' },
    collect: monitor => ({
      isDrag: monitor.isDragging()
    })
  });

  dragRef(drop(ref));

  return (
    <li className={`${styles.constructorElem} ${isDrag && styles.isDrag}`} ref={ref} >
      <div className={`${styles.dragIcon}`}>
        <DragIcon type="primary" />
      </div>
      <ConstructorElement
        text={name}
        price={price}
        thumbnail={image}
        handleClose={() => onHandleClose(uniqId)} />
    </li>
  )
}













