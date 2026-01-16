const CONTENT_CLASSNAME = 'collected-content';

// Class defines a custom element
class CollectMoneyModal extends HTMLElement {

    constructor() {
        super();
        this.value = 1110;
        this.remain = 230;
        this.note = '';
    
    }

    /**
     * Browser calls this method when the element is added to the document
     * ComponentDidMount in ReactJS
     * ngOnInit in Angular
     */
    connectedCallback() {
        this.render();
    }

    handleModalClick(event) {
        let closed = true;

        const paths = event.path;
        Array.from(paths).forEach(path => {
            let classes = Array.from(path.classList || []);
            classes.forEach(cl => {
                if(cl === CONTENT_CLASSNAME) {
                    closed = false;
                }
            })
        });

        if(closed) {
            const overlay = document.querySelector('#overlay');
            for(let node of overlay.childNodes) {
                overlay.removeChild(node)
            }
        }
    }

    /**
     * Browser calls this methods when the element is removed from the document
     * clearInterval, clearTimeout and unsubscription should be called here
     * componentWillUnmount in ReactJS
     * ngOnDestroy in Angular
     */
    disconnectedCallback() {
        this.removeEventListener('click', this.handleModalClick);
    }

    /**
     * Get an array of attribute names to monitor for changes
     */
    static get observedAttributes() {
        return ['dealFinId'];
    }

    /**
     * Called when one of above attributes changed
     * shouldComponentUpdate in reactJS
     * ngOnChange in Angular
     */
    attributeChangedCallback(name, oldValue, newValue) {

        // If attributes change, call render function to update view
        if(oldValue !== newValue ) {
            //console.log('Calling render')
            this.render();
        }
    }

    render() {
        this.innerHTML = `
            <div class="collected-content">
                <div class="row">Thu tiền</div>
                <div class="row">
                    <div class="col-sm-6">Doanh thu: ${this.value}</div>
                    <div class="col-sm-6">Còn lại: ${this.remain}</div>
                </div>

                <div class="row">
                    <div class="col-sm-3">Note:</div>
                    <div class="col-sm-9">
                        <textarea id="note"></textarea>
                    </div>
                </div>

                <div class="row">
                    <button id="collect">Thu tiền</button>
                </div>
            </div>
        `

        this.setAttribute('class', 'container');

        this.addEventListener('click', this.handleModalClick);

        const note = this.querySelector('#note');
        note.addEventListener('keyup', (e) => {
            this.note = e.target.value
        });

        const collectBtn = this.querySelector('#collect');
        collectBtn.addEventListener('click', () => {
            console.log(this.note);
        })
    }
}

customElements.define('collect-money-modal', CollectMoneyModal);

function handleClick(id) {
    const modal = document.createElement('collect-money-modal');
    modal.setAttribute('active', true);
    modal.setAttribute('dealFinId', id);
    const overlay = document.querySelector('#overlay');
    // Get all child element of overlay
    for(let node of overlay.childNodes) {
        overlay.removeChild(node)
    }
    overlay.appendChild(modal);
}