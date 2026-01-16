(function() {
  function ModalCtrl($uibModalInstance, Room) {

    /**
    * @func addRoom
    * @desc If room, add to rooms array in firebase & close modal
    * @param {String}
    */
    this.addRoom = function(room) {
      if (room) {
        Room.add(room);
        this.cancel();
      }
    };

    /**
    * @func cancel
    * @desc Close modal
    */
    this.cancel = function () {
      $uibModalInstance.close();
    };
  }

  angular
    .module('blocChat')
    .controller('ModalCtrl', ['$uibModalInstance', 'Room', ModalCtrl]);
})();