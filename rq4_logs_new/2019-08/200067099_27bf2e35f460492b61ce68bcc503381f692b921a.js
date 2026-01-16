function BrowserDetection() {
            var allGood = false;
            //Check if browser is IE
            console.log(navigator.userAgent);
            console.log((navigator.userAgent.search("Opera") >= 0 || navigator.userAgent.search("OPR") >= 0))
            //Check if browser is Chrome
            if (navigator.userAgent.search("Chrome") >= 0) {
                // insert conditional Chrome code here
                allGood = true;
            }
            //Check if browser is Firefox 
            if (navigator.userAgent.search("Firefox") >= 0) {
                // insert conditional Firefox Code here
                allGood = true;
            }
            //Check if browser is Safari
            if (navigator.userAgent.search("Safari") >= 0 && navigator.userAgent.search("Chrome") < 0) {
                // insert conditional Safari code here
                allGood = true;
            }
            //Check if browser is Opera
            if ((navigator.userAgent.search("Opera") >= 0) || (navigator.userAgent.search("OPR") >= 0)) {
                console.log('BF In Opera = ' + allGood);
                // insert conditional Opera code here
                allGood = false;
                console.log('AF In Opera = ' + allGood);
            }
            if (navigator.userAgent.search("MSIE") >= 0) {
                // insert conditional IE code here
                allGood = false;
            }
            if (navigator.userAgent.search("Edge") >= 0) {
                // insert conditional Edge code here
                allGood = false;
            }
            
            console.log('result = ' + allGood);
            if (!allGood) {

            //// what ever you want to do. In my case, I am displaying a bootstrap modal
                $('#IE_EdgeInfoModal').modal('show');

            }
        }
