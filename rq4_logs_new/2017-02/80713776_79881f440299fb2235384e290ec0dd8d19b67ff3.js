(function () {
    "use strict";

    angular
        .module("calculator")
        .controller("HistoryModalController", HistoryModalController);

    HistoryModalController.$inject = ["solveFactory", "$window", "$uibModal", "$uibModalInstance"];

    function HistoryModalController(solveFactory, $window, $uibModal, $uibModalInstance) {
        var vm = this;
        var answerHistory = solveFactory.answerHistory;
        var questionHistory = solveFactory.questionHistory;

        vm.solveFactory = solveFactory;
        vm.answerHistory = answerHistory;
        vm.questionHistory = questionHistory;
        vm.clearHistory = clearHistory;
        vm.sendDataInput = sendDataInput;

        //call solveFactory and clear the history
        function clearHistory() {
            console.log(answerHistory);
            console.log(questionHistory);

            if (vm.answerHistory.length === 0 || vm.questionHistory.length === 0)
                $window.confirm("Nothing to delete");

            else if ($window.confirm("Are you sure you want to clear history? All data will be lost.")) {
                solveFactory.clearHistory(); //clear the data in history

                vm.answerHistory = solveFactory.answerHistory; //set answerHistory to empty array (after cleared)
                vm.questionHistory = solveFactory.questionHistory; //do the sdame to question history

                vm.solveFactory.calculate = ""; //set to empty since they cleared data

                //user has confirmed that they want the modal closed
                $uibModalInstance.close('cancel');
            }
        }

        function sendDataInput(data) {

            console.log(vm.answerHistory);
            console.log(vm.questionHistory);

            console.log("historyModal sendData. Data is: " + data + "\n");

            //if it is not a number or string then set calculate to empty string
            if (data === "NaN" || isNaN(data) || data === undefined)
                vm.solveFactory.calculate = ""; //give it an empty string
        
            else
                vm.solveFactory.calculate = data; //give the value to calculate that was passed in

            $uibModalInstance.close("data passed in");
            
        }
    }
})();