import {
  createElement,
  ForwardedRef,
  forwardRef,
  ReactNode,
  useEffect,
  useState,
} from 'react';
import { DynamicProps, HTMLTags } from '../../utils/types/DynamicProps';

function Alert<T extends HTMLTags = 'div'>(
  props: DynamicProps<T>,
  ref: ForwardedRef<HTMLDivElement>
) {
  const { as = 'div', children, ...rest } = props;
  const [content, setContent] = useState<ReactNode | null>(null);

  useEffect(() => {
    // makes sure that the container exist before appending content
    // to ensure that screen readers detect and read the content
    const timer = setTimeout(() => setContent(children), 100);

    return () => {
      clearTimeout(timer);
    };
  }, [children]);

  return createElement(
    as,
    {
      'aria-live': 'polite',
      'aria-atomic': 'true',
      ...rest,
      ref,
    },
    content
  );
}

export default forwardRef(Alert);