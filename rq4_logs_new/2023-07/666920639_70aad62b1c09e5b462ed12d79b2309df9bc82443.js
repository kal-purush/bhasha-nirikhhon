import { useState, useEffect, useRef } from 'react';
export default function Textarea() {
  const [state, setState] = useState([null])
  const counter = useRef(0);
  const selection = useRef('');
  useEffect(() => {
    var storage = window.localStorage.getItem("storage") ? window.localStorage.getItem("storage") : null
    if (storage) document.documentElement.innerHTML = storage
    window.addEventListener('load', initFocus)
    document.addEventListener('selectionchange', handleSelection);
    document.addEventListener('keydown', handleKeydown);
    document.addEventListener('keyup', handleKeyup);
    function handleKeydown(e) {
      if (e.key === 'Backspace' && document.activeElement.innerText == '' && document.activeElement.id) {
        var id = document.activeElement.id
        placeCaretAtEnd(document.activeElement.previousSibling)
        if (document.querySelector(`a.${id}`)) document.querySelector(`a.${id}`).replaceWith(id)
        let newState = state.filter(function (item) {
          return item !== id
        })
        setState(newState)
      }
    }
    function handleKeyup() {
      window.localStorage.setItem("storage", document.documentElement.innerHTML)
      console.log('saved');
    }
    return () => {
      window.removeEventListener('load', initFocus)
      document.removeEventListener('selectionchange', handleSelection);
      document.removeEventListener('keydown', handleKeydown);
      document.removeEventListener('keyup', handleKeyup);
    }
  })
  function handleSelection() {
    const currentSelection = document.getSelection()
    if (currentSelection) {
      selection.current = currentSelection;
    }
  }
  function initFocus() {
    setTimeout(function () {
      document.querySelectorAll('.div')[0].focus();
    }, 0);
  }

  return (
    <>
      {state.map((v, i) => <div onKeyDown={(e) => link(e, i)} id={v} data-index={i} key={i} spellCheck='false' className={'div ' + v} contentEditable='true' tabIndex='1'>{v}</div>)}
    </>
  )

  function link(e) {
    let id = selection.current.toString()
    if (e.ctrlKey && e.key === 'l' && state.includes(id) == false && id == encodeURIComponent(id)) {
      replaceTextWithLink(id)
      setState([...state, id])
      addStyle(id)
      e.preventDefault()
    }
  }

  function addStyle(className) {
    var styleSheet = document.createElement("style")
    var hue = `hsl(${counter.current}, 50%, 50%)`
    styleSheet.innerText = `a.${className}{border-bottom:solid ${hue};}div.${className}{border-left:solid ${hue};}`
    document.head.appendChild(styleSheet)
    counter.current += 207
  }

  function replaceTextWithLink(id) {
    var sel, range;
    let a = document.createElement('a')
    a.href = '#' + id
    a.className = a.innerText = id
    if (window.getSelection) {
      sel = window.getSelection();
      if (sel.rangeCount) {
        range = sel.getRangeAt(0);
        range.deleteContents();
        range.insertNode(a);
      }
    } else if (document.selection && document.selection.createRange) {
      range = document.selection.createRange();
      range.text = a;
    }
  }

  function placeCaretAtEnd(el) {
    el.focus();
    if (typeof window.getSelection != "undefined"
      && typeof document.createRange != "undefined") {
      var range = document.createRange();
      range.selectNodeContents(el);
      range.collapse(false);
      var sel = window.getSelection();
      sel.removeAllRanges();
      sel.addRange(range);
    } else if (typeof document.body.createTextRange != "undefined") {
      var textRange = document.body.createTextRange();
      textRange.moveToElementText(el);
      textRange.collapse(false);
      textRange.select();
    }
  }
}