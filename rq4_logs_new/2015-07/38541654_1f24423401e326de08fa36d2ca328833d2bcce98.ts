interface Listener {
  callback: Function;
  thisArg?: any;
}
class EventEmitter {
  events: {[index: string]: Listener[]} = {};
  constructor() { }
  on(name: string, callback: Function, context?: any): EventEmitter {
    if (this.events[name] === undefined) {
      this.events[name] = [];
    }
    this.events[name].push({callback: callback, thisArg: context});
    return this;
  }
  /**
  Does not raise an error if the given event does not exist, or the given
  callback cannot be found.
  */
  off(name: string, callback: Function): EventEmitter {
    if (name in this.events) {
      var listeners = this.events[name];
      for (var i = 0, listener: Listener; (listener = listeners[i]); i++) {
        if (listener.callback === callback) {
          listeners.splice(i, 1);
          // only remove one at a time
          break;
        }
      }
    }
    return this;
  }
  /**
  Does not raise an error if there are no listeners for the given event.
  */
  emit(name: string, ...args: any[]): EventEmitter {
    if (name in this.events) {
      var listeners = this.events[name];
      for (var i = 0, listener: Listener; (listener = listeners[i]); i++) {
        listener.callback.apply(listener.thisArg, args);
      }
    }
    return this;
  }
}

/**
input_el:
    the original text input element that listens for input
results_el:
    the container element for results
selected_el:
    the currently element for results
*/
interface DropdownOption {
  label: string | Node;
  value: string;
}
export class Dropdown extends EventEmitter {
  results_el: HTMLUListElement;
  selected_el: HTMLLIElement;
  private query: string;
  constructor(public input_el: HTMLInputElement) {
    super();
    // DOMLib.EventEmitter.call(this);
    // initialize results element, even though we don't use it yet
    this.results_el = document.createElement('ul');
    this.results_el.setAttribute('class', 'dropdown');
    this.results_el.setAttribute('style', 'position: absolute; display: none');
    // insert results element as a sibling to the input element
    //   if input_el.nextSibling is null, this works just like .appendChild
    this.input_el.parentNode.insertBefore(this.results_el, this.input_el.nextSibling);
    // attach events
    this.input_el.addEventListener('keydown', (ev) => this.keydown(ev));
    this.input_el.addEventListener('keyup', (ev) => this.keyup(ev));
    // simply clicking or tabbing into the box should trigger the dropdown
    this.input_el.addEventListener('focus', () => this.changed());
    this.input_el.addEventListener('blur', () => this.reset());
  }
  keydown(event) {
    if (event.which === 13) { // 13 == ENTER
      event.preventDefault();
    }
    else if (event.which == 38) { // 38 == UP-arrow
      if (this.selected_el) {
        // Don't go lower than 0 when clicking up
        if (this.selected_el.previousSibling) {
          this.preselect(<HTMLLIElement>this.selected_el.previousSibling);
        }
      }
      event.preventDefault();
    }
    else if (event.which == 40) { // 40 == DOWN-arrow
      event.preventDefault();
      if (this.selected_el) {
        // Don't go higher than the last available option when going down
        if (this.selected_el.nextSibling) {
          this.preselect(<HTMLLIElement>this.selected_el.nextSibling);
        }
      }
      else {
        this.preselect(<HTMLLIElement>this.results_el.firstChild);
      }
    }
  }
  keyup(event) {
    if (event.which === 13) { // 13 == ENTER
      this.selected();
      this.reset();
    }
    else {
      this.changed();
    }
  }
  selected() {
    this.emit('select', this.selected_el.dataset['value'], this.selected_el);
  }
  changed() {
    var query = this.input_el.value;
    if (query !== this.query) {
      this.emit('change', query);
    }
  }
  preselect(el: HTMLLIElement) {
    if (this.selected_el) {
      this.selected_el.classList.remove('selected');
    }
    if (el) {
      el.classList.add('selected');
      this.emit('preselect', el.dataset['value']);
    }
    this.selected_el = el;
  }
  reset() {
    this.results_el.style.display = 'none';
    // is this the best place for this reset?
    this.query = null;
  }
  setOptions(options: DropdownOption[], query: string) {
    // set this.query so that we know when the input differs from the current query
    this.query = query;

    // clear
    var results_el = <HTMLUListElement>this.results_el.cloneNode(false);
    results_el.style.display = options.length > 0 ? 'block' : 'none';
    // while (this.results_el.lastChild) {
    //   results_el.removeChild(this.results_el.lastChild);
    // }
    options.forEach(function(option) {
      // label can be either a string or a DOM element
      var label: Node;
      if (typeof option.label === 'string') {
        label = document.createTextNode(<string>option.label);
      }
      else {
        label = <Node>option.label;
      }
      var li = document.createElement('li');
      li.appendChild(label);
      li.dataset['value'] = option.value;

      // I wish I could listen for mouseover / mousedown higher up, but it's
      // harder, since it's hard to listen at a certain level
      li.addEventListener('mouseover', () => this.preselect(li));
      li.addEventListener('mousedown', () => {
        this.preselect(li);
        this.selected();
      });
      results_el.appendChild(li);
    });

    results_el.addEventListener('mouseout', () => this.preselect(null));

    // MDN: var replacedNode = parentNode.replaceChild(newChild, oldChild);
    this.results_el.parentNode.replaceChild(results_el, this.results_el);
    // and set the current results element to the new one
    this.results_el = results_el;
    this.selected_el = undefined;
  }

  /** Dropdown.attach is the preferred API endpoint for initializing an
  dropdown element. It sets up the controller object, all the listeners,
  and returns the results list element, which has some custom event listeners.
  */
  static attach(input_el: HTMLInputElement) {
    return new Dropdown(input_el);
  }
}