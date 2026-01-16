
class HeroModalController {
  constructor($uibModalInstance, items) {
    this.$uibModalInstance = $uibModalInstance;
    this.items = items;
    this.selected = {};

  }

  ok(){
    console.log('called ok');
    this.$uibModalInstance.close();
  }

  cancel(){
    this.$uibModalInstance.close();
  }

}

HeroModalController.$inject = ['$uibModalInstance','items'];

export default HeroModalController;