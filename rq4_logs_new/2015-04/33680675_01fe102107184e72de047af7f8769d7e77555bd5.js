<script type="text/javascript" src="{PHPWS_SOURCE_HTTP}mod/plm/javascript/details/view.js">
</script>

<script type="text/javascript">
    $(document).ready(function(){
        var semaphore = new Semaphore(1);
        // Create objects
        $('.actor-details').each(function(index){
            new Details(this, semaphore);
        });
    });
</script>