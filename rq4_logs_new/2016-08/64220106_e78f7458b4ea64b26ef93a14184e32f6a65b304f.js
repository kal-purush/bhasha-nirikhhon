function hasClass(element, className) {
    return element.className && new RegExp("(^|\\s)" + className + "(\\s|$)").test(element.className);
}

Element.prototype.hasClass = function(className) {
    return this.className && new RegExp("(^|\\s)" + className + "(\\s|$)").test(this.className);
};

/* Navbar javascript */

(function() {    
    
    var getCollapseElement = function(el) {
        var isCollapse = el.getAttribute('data-collapse');
        if (isCollapse !== null) {
            return el;
        } else {
            var isParentCollapse = el.parentNode.getAttribute('data-collapse');
            return (isParentCollapse !== null) ? el.parentNode : undefined;
        }
    };
    
    var collapseClickHandler = function(event) {
        var triggerEl = getCollapseElement(event.target);
        if (triggerEl === undefined) {
            event.preventDefault();
            return false;
        }
        var targetEl = document.querySelector(triggerEl.getAttribute('data-target'));
        if (targetEl) {
            triggerEl.classList.toggle('active');
            targetEl.classList.toggle('on');
        }
    };
    
    document.addEventListener('click', collapseClickHandler, false);
    
    var getActiveMenuElement = function(el) {
        var isActive = el.getElementsByClassName('nav-link active');
        return (isActive.length > 0) ? isActive: undefined;        
    }
    
    var navbarClickHandler = function(event) {
        var triggerEl = event.target;
        if (triggerEl != undefined) {
            if(triggerEl.hasClass('nav-link')){
                var parentEl = triggerEl.parentNode;
                var active = getActiveMenuElement(parentEl);
                if (active != undefined) {
                    active[0].classList.remove("active");
                }
                triggerEl.classList.add("active");
            }            
        }
    }
    
    document.addEventListener('click', navbarClickHandler, false);
    
})(document, window);