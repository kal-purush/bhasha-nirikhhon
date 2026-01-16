import {
  Children,
  cloneElement,
  createElement,
  Fragment,
  MouseEvent,
  ReactElement,
  useEffect,
  useState,
} from 'react';
import { AccordionProps } from '../../atoms/accordion/Accordion';
import { callAll } from '../../utils/functions/callAll';

type AccordionGroupProps = {
  initial?: number;
  open?: number;
  onClick?: (event: MouseEvent, open: number) => void;
  children: Array<ReactElement<AccordionProps>>;
};

function AccordionGroup(props: AccordionGroupProps) {
  const {
    children,
    initial = -1,
    open: controlledOpen,
    onClick: controlledOnClick,
  } = props;
  const [open, setOpen] = useState(initial);

  function onClick(index: number) {
    return function (event: MouseEvent, request: boolean) {
      if (controlledOpen !== undefined) {
        if (controlledOnClick === undefined) {
          console.warn(
            '"onClick" handler is undefined while the component is in controlled mode. The click event recently received will be ignored'
          );
        } else {
          controlledOnClick(event, index);
        }
      } else {
        setOpen(() => (request ? index : -1));
      }
    };
  }

  useEffect(() => {
    if (controlledOpen !== undefined) {
      setOpen(() => controlledOpen);
    }
  }, [controlledOpen, setOpen]);

  return createElement(
    Fragment,
    null,
    Children.map<ReactElement<AccordionProps>, ReactElement<AccordionProps>>(
      children,
      (child, index) =>
        cloneElement(child, {
          ...child.props,
          open: open === index,
          onClick: callAll(onClick(index), child.props.onClick),
        })
    )
  );
}

export default AccordionGroup;