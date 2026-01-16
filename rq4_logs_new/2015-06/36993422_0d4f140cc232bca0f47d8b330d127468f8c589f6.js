/**
 * Created by david on 6/8/15.
 */

'use strict';

function userInfoCtrl(userService, MessagingService, $state) {
    var self = this;

    self.selected = null;
    self.users = [];

    self.selected = MessagingService.getSelectedUser();
    self.backToMainContent = backToMainContent;
    //self.selected.likesWine = false;
    //self.selected.likesBeer = false;
    //self.selected.likesVodka = false;
    //self.selected.likesRum = false;
    //self.selected.likesTequila = false;
    //self.selected.likesWhiskey = false;
    //
    //self.selected.alsoLikes = ['Hiking', 'Movies', 'Camping'];
    self.roAlsoLikes = angular.copy(self.selected.alsoLikes);

    function backToMainContent(){
        if(self.selected) {
            userService.updateUser(self.selected);
        }
        $state.go('/');
    }

}

module.exports = userInfoCtrl;

