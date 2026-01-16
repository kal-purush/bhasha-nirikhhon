function openPopupCenter(url, title, w, h) {
    // Fixes dual-screen position
    // Most browsers use window.screenLeft
    // Firefox uses screen.left
    var dualScreenLeft = getFirstNumber(window.screenLeft, screen.left),
      dualScreenTop = getFirstNumber(window.screenTop, screen.top),
      width = getFirstNumber(window.innerWidth, document.documentElement.clientWidth, screen.width),
      height = getFirstNumber(window.innerHeight, document.documentElement.clientHeight, screen.height),
      left = ((width / 2) - (w / 2)) + dualScreenLeft,
      top = ((height / 2) - (h / 2)) + dualScreenTop,
      newWindow = window.open(url, title, getSpecs());
  
    // Puts focus on the newWindow
    if (window.focus) {
      newWindow.focus();
    }
  
    return newWindow;
  
    function getSpecs() {
      return 'scrollbars=yes, width=' + w + ', height=' + h + ', top=' + top + ', left=' + left;
    }
  
    function getFirstNumber() {
      for(var i = 0, len = arguments.length; i < len; i++) {
        var value = arguments[i];
  
        if (typeof value === 'number') {
          return value;
        }
      }
    }
  }


var VelixID = {
    init: function (params) {
        this._token = params.token;

        document.querySelectorAll('[data-button-type=velixid-login]').forEach(function (button) {
            this._styleButton(button);
            this._addBehaviour(button);
        }.bind(this));

        window.addEventListener('message', function(e) {
            if (this._popup && e.data.close) {
                if (e.data.data === 'cancel') {
                    this._popup.close();
                    this._fireEvent('cancel', undefined);
                } else {
                    setTimeout(function () {
                        this._popup.close();
                    }.bind(this), 3000);

                    if (e.data.data === 'rejected') {
                        this._fireEvent('reject', undefined);
                    } else {
                        this._fireEvent('login', e.data);
                    }
                }
            }
        }.bind(this) , false);
    },

    _styleButton: function (button) {
        button.style.padding = '8px 15px'
        button.style.background = '#090b23';
        button.style.color = '#ffffff';
        button.style.font = '#ffffff';
        button.style.fontFamily = 'Helvetica, Lato, Arial';
        button.style.fontSize = '12px';
        button.style.border = '0px solid';
        button.style.borderRadius = '4px';
        button.style.outline = 'none';
        button.style.cursor = 'pointer';
    },
    
    _addBehaviour: function (button) {
        switch (button.dataset['buttonType']) {
            case 'velixid-login':
                button.onclick = this._openLoginPopup.bind(this);
                break;
            default:
                break;
        }
    },

    _openLoginPopup: function () {
        this._popup = openPopupCenter('https://api.velix.id/popup/login/' + this._token, 'Login with Velix.ID', 500, 500);
    },

    _events: {
        'login': [],
        'error': [],
        'reject': [],
        'cancel': []
    },

    _fireEvent: function (event, data) {
        if(this._events[event]) {
            for (let index = 0; index < this._events[event].length; index++) {
                let callback = this._events[event][index];
                callback(data);
            }
        }
    },

    on: function(event, callback) {
        if(this._events[event] && typeof callback === 'function') {
            this._events[event].push(callback);
        }
    }
}