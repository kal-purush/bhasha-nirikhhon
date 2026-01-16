import {
  createElement,
  createContext,
  useState,
  ComponentProps,
  useContext,
} from 'react';
import useId from '../../utils/hooks/useId';

type Context = {
  id: string;
  open: boolean;
  onClick: () => void;
};

const AccordionContext = createContext<Context | null>(null);

function useAccordion(): Context {
  const context = useContext(AccordionContext);

  if (context === null) {
    throw new Error('Component must be wrapped by "Accordion"');
  }

  return context;
}

function Accordion({ ...rest }: ComponentProps<'div'>) {
  const id = useId('accordion');
  const [open, setOpen] = useState(false);

  function onClick() {
    setOpen((prev) => !prev);
  }

  return createElement(
    AccordionContext.Provider,
    {
      value: { id, open, onClick },
    },
    createElement('div', rest)
  );
}

Accordion.Header = function Header(props: ComponentProps<'button'>) {
  const { id, open, onClick } = useAccordion();
  return createElement('button', {
    id: `${id}_button`,
    'aria-controls': `${id}_content`,
    'aria-expanded': open,
    onClick,
    ...props,
  });
};

Accordion.Panel = function Panel(props: ComponentProps<'section'>) {
  const { id, open } = useAccordion();
  return createElement('section', {
    id: `${id}_content`,
    'aria-labelledby': `${id}_button`,
    hidden: !open,
    ...props,
  });
};

export default Accordion;