getScreenSize: function() {
  if (window.matchMedia("(min-width: 0px) and (max-width: 575px)").matches) {
    return 'xs';
  }
  else if (window.matchMedia("(min-width: 576px) and (max-width: 767px)").matches) {
    return 'sm';
  }
  else if (window.matchMedia("(min-width: 768px) and (max-width: 991px)").matches) {
    return 'md';
  }
  else if  (window.matchMedia("(min-width: 992px) and (max-width: 1199px)").matches) {
    return 'lg';
  }    
  else if  (window.matchMedia("(min-width: 1200px) and (max-width: 1399px)").matches) {
    return 'xl';
  }    
  else if  (window.matchMedia("(min-width: 1400px)").matches) {
    return 'xx';
  }
  else {
    return '??';
  }        
}