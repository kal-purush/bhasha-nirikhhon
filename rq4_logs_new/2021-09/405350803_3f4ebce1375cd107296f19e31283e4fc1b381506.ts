//useDocumentTitle.ts
import { useRef, useEffect }  from "react";

export function useDocumentTitle(title, prevailOnUnknown = false) {
  const defaultTitle = useRef(document.title);

  useEffect(() => {
    document.title = title;
  }, [title]);

  useEffect(()=>{
    if(!prevailOnUnknown){
      document.title = defaultTitle.current;
    }
  }, [prevailOnUnknown])
}
