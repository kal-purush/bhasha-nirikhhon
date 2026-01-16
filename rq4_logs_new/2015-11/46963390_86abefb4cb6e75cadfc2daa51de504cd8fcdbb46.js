 /**
 * ownCloud - Files_External_Aws 
 *
 * This file is licensed under the Affero General Public License version 3 or
 * later. See the COPYING file.
 *
 * @author Simon <simon.l@inwinstack.com>
 * @copyright Simon 2015
 */
(function ($, OC) { 
    
    function getData(user) {
        return  $.ajax({	    
           url: OC.generateUrl('/apps/files_external_aws/getSizeFromFilecache'),
           method: 'GET',
           datatype: 'json',
           data: {user: user}
        });    
    }
    
    /*
    * @param Array an object array.
    * @return Object key is username, value is usage.
    */
    function transObjToArray(result) {
        var map = {};
        result.forEach(function(v,i) {
            map[v.username] = v.usage;
        });
        
        return map;
    }
    
    function render(current_row, data, name) {
        current_row.find('.usage div').replaceWith(data[name]);
    }

    function userCreated(usage, name) {
        $('#userlist tbody tr:visible').each(function() {
            if($(this).data('uid') === name) {
               var current_row = $(this);
                
               render(current_row, usage, name);
                
               return false;
            }
        });
    }
    
    function userListLoaded(data) {
        $('#userlist tbody tr:visible').each(function() {
            var current_row = $(this);
            var name =  current_row.find('.name').text();
            
            render(current_row, data, name);
        });
    } 
    
    ajaxSuccess.bind('GET:/settings/users/users', function() {
        getData().done(function(result) {
            userListLoaded(result);
        });
    });
    
    ajaxSuccess.bind('POST:/settings/users/users', function(event) {
        var user =  event.xhr.responseJSON.name;
        getData(user).done(function(result) {
            userCreated(result, user);
        });
    });

    $(function() {
        var cell_usage = $('<td>').attr({class:'usage'});
        var loader = $('<div>').attr({class:'loading-usages'});
        var translation = t('files_external_aws', 'Usage');
        var th = $('<th>').attr({width: '150px'});
        
        cell_usage.append(loader);
        $('#userlist tbody tr:hidden .quota').after(cell_usage);
        $('#userlist thead tr #headerQuota').after(th.text(translation));
    });

})(jQuery, OC);    