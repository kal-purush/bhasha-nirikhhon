    var d_flag = true;
    var ele = document.getElementsByClassName('boom');
    var logo =  document.getElementById('logo');

    setTimeout(function() { boomOpen(); }, 2000);

    function boomOpen(){
        if(d_flag)
        {   
            d_flag = false
            logo.classList.add('bright')
            for(var i=0;i<ele.length;i++)
            {
            ele[i].classList.add('bubble')
            }
        }
        else
        {   
            d_flag = true
            logo.classList.remove('bright')
            for(var i=0;i<ele.length;i++)
            {
            ele[i].classList.remove('bubble')
            }
        }
        
        
    }