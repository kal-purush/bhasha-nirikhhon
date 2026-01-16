
    var $collectionHolder;

    // setup an "add a tag" link
    var $addButton = $('<a href="#" class="add_tag_link btn btn-default">Add a SubEntry</a>');
    var $entries = $('.formfinal').append($addButton);

    jQuery(document).ready(function() {
        // Get the ul that holds the collection of tags
        $collectionHolder = $('div.entries');
        
        // add a delete link to all of the existing tag form li elements
        $collectionHolder.find('div.entry').each(function() {
            addTagFormDeleteLink($(this));
        });

        // add the "add a tag" anchor and li to the tags ul
        $collectionHolder.append($entries);

        // count the current form inputs we have (e.g. 2), use that as the new
        // index when inserting a new item (e.g. 2)
        $collectionHolder.data('index', $collectionHolder.find(':input').length);

        $addButton.on('click', function(e) {
            // prevent the link from creating a "#" on the URL
            e.preventDefault();

            // add a new tag form (see next code block)
            addTagForm($collectionHolder, $entries);
        });
    });
    
    function addTagForm($collectionHolder, $entries) 
    {
        // Get the data-prototype explained earlier
        var prototype = $collectionHolder.data('prototype');

        // get the new index
        var index = $collectionHolder.data('index');

        // Replace '__name__' in the prototype's HTML to
        // instead be a number based on how many items we have
        var newForm = prototype.replace(/__name__/g, index);
        //newForm.addClass('.entry .col-md-4');
        
        //var newForm = newForm.replace(/<div/i,'<div class="entry col-md-4"');
        //$('div.entry_subEntries_9').addClass('.entry .col-md-4');
        //console.log(newForm);
        
        // increase the index with one for the next item
        $collectionHolder.data('index', index + 1);

        // Display the form in the page in an li, before the "Add a tag" link li
        var $entry = $('<div class="entry col-md-4"></div>').append(newForm);
        
        console.log($entry);
        $entries.before($entry);
        
        // handle the removal for this newly created divTag, this cannot be done
        addTagFormDeleteLink($entry);
    }

    //Handles the Removal for existing div Tags
    function addTagFormDeleteLink($entryDiv) 
    {
        var $removeFormA = $('<a href="#" class="remove_tag btn btn-default">Delete</a>');
                
        $entryDiv.append($removeFormA);

        $removeFormA.on('click', function(e) {
            // prevent the link from creating a "#" on the URL
            e.preventDefault();

            // remove the li for the tag form
            $entryDiv.remove();
        });
    }
    


