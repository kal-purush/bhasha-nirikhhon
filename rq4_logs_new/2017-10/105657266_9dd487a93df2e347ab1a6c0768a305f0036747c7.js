describe("FornecedoresController", function() {
  var fornecedores = mockData.getFornecedores();
  var controller;
  var $scope;
  beforeEach(function() {
    bard.appModule("appMobStudio");
    bard.inject(
      "$controller",
      "$q",
      "$rootScope",
      "fornecedorData",
      "msgs",
      "tabs",
      "consts"
    );
  });

  beforeEach(function() {
    sinon.stub(fornecedorData, "getFornecedores").returns(
      $q.when({
        data: fornecedores
      })
    );
    sinon.stub(fornecedorData, "updateFornecedor").returns($q.when({}));
    sinon.stub(fornecedorData, "deleteFornecedor").returns($q.when({}));
    sinon.stub(fornecedorData, "createFornecedor").returns($q.when({ _id: 10 }));
    sinon.stub(fornecedorData, "getCount").returns(
      $q.when({
        data: { value: 15 }
      })
    );
    sinon.stub(msgs, "addSuccess").returns("");
    $scope = $rootScope.$new();
    controller = $controller("fornecedoresController", {
      $scope: $scope
    });
  });

  bard.verifyNoOutstandingHttpRequests();

  describe("Fornecedores Controller", function() {
    it("Deve ser criado", function() {
      expect(controller).to.be.defined;
    });

    describe("Após ativação", function() {
      beforeEach(function() {
        $rootScope.$apply();
      });
      it("Deve possuir total de páginas", function() {
        expect($scope.pages).to.be.defined;
      });

      it("Total de páginas deve ser 2", function() {
        expect($scope.pages).to.equal(2);
      });

      it("Deve possuir lista de fornecedores", function() {
        expect($scope.fornecedores).to.have.length(fornecedores.length);
      });

      it("Fornecedor deve estar definido", function() {
        expect($scope.fornecedor).to.be.defined;
      });

      it("Fornecedor deve ter endereços inicializados", function() {
        expect($scope.fornecedor.Enderecos).to.be.defined;
      });

      it("Aba lista deve estar visível", function() {
        expect($scope.tabList).to.be.true;
      });
    });

    describe("Ao editar um fornecedor", function() {
      beforeEach(function() {
        $rootScope.$apply();
        $scope.showTabUpdate(fornecedores[0]);
      });

      it("Fornecedor selecionado deve estar carregado", function() {
        expect($scope.fornecedor._id).to.be.equal(fornecedores[0]._id);
      });

      it("Aba editar deve estar visível", function() {
        expect($scope.tabUpdate).to.be.true;
      });
    });

    describe("Ao criar um fornecedor novo", function() {
      beforeEach(function() {
        $rootScope.$apply();
        $scope.fornecedor.Nome = "Teste";
        $scope.createFornecedor();
        $scope.$apply();
      });

      it("Serviço de dados deve ser acionado", function() {
        expect(fornecedorData.createFornecedor.getCall(0).args[0].Nome).to.equal(
          "Teste"
        );
      });

      it("Fornecedor carregado deve estar redefinido", function() {
        expect($scope.fornecedor._id).to.be.undefined;
      });

      it("Fornecedor deve ter endereços inicializados", function() {
        expect($scope.fornecedor.Enderecos).to.be.defined;
      });

      it("Mensagem de sucesso deve ser apresentada", function() {
        $scope.$apply();
        expect(msgs.addSuccess.getCall(0).args[0]).to.equal(
          "Fornecedor incluído com sucesso!"
        );
      });
    });

    describe("Ao salvar um fornecedor alterado", function() {
      beforeEach(function() {
        $rootScope.$apply();
        $scope.showTabUpdate(fornecedores[0]);
        $scope.updateFornecedor();
      });

      it("Serviço de dados deve ser acionado", function() {
        expect(fornecedorData.updateFornecedor.getCall(0).args[0]).to.equal(
          $scope.fornecedor
        );
      });

      it("Fornecedor carregado deve estar redefinido", function() {
        $scope.$apply();
        expect($scope.fornecedor._id).to.be.undefined;
      });

      it("Mensagem de sucesso deve ser apresentada", function() {
        $scope.$apply();
        expect(msgs.addSuccess.getCall(0).args[0]).to.equal(
          "Fornecedor atualizado com sucesso!"
        );
      });
    });

    describe("Ao cancelar uma alteração", function() {
      beforeEach(function() {
        $rootScope.$apply();
        $scope.showTabUpdate(fornecedores[0]);
        $scope.cancel();
      });

      it("Fornecedor deve ser redefinido", function() {
        expect($scope.fornecedor._id).to.be.undefined;
      });

      it("Aba lista deve estar visível", function() {
        expect($scope.tabList).to.be.true;
      });
    });

    describe("Ao excluir um fornecedor", function() {
      beforeEach(function() {
        $rootScope.$apply();
        $scope.showTabDelete(fornecedores[0]);
        $scope.deleteFornecedor();
      });

      it("Aba excluir deve estar visível", function() {
        expect($scope.tabDelete).to.be.true;
      });

      it("Fornecedor selecionado deve estar carregado", function() {
        expect($scope.fornecedor._id).to.be.equal(fornecedores[0]._id);
      });

      it("Serviço de dados deve ser acionado", function() {
        expect(fornecedorData.deleteFornecedor.getCall(0).args[0]).to.equal(
          $scope.fornecedor
        );
      });

      describe("Ao confirmar a exclusão de um fornecedor", function() {
        beforeEach(function() {
          $scope.$apply();
        });

        it("Fornecedor deve ser redefinido", function() {
          expect($scope.fornecedor._id).to.be.undefined;
        });

        it("Fornecedor deve ter endereços inicializados", function() {
          expect($scope.fornecedor.Enderecos).to.be.defined;
        });

        it("Aba lista deve estar visível", function() {
          expect($scope.tabList).to.be.true;
        });

        it("Mensagem de sucesso deve ser apresentada", function() {
          expect(msgs.addSuccess.getCall(0).args[0]).to.equal(
            "Fornecedor excluído com sucesso!"
          );
        });
      });

      describe("Ao adicionar um endereço", function() {
        beforeEach(function() {
          $rootScope.$apply();
          $scope.addEndereco(0);
        });

        it("Lista de endereços deve conter 2 elementos", function() {
          expect($scope.fornecedor.Enderecos).to.have.length(2);
        });

        describe("Ao excluir um endereço", function() {
          beforeEach(function() {
            $scope.deleteEndereco(1);
          });

          it("Lista de endereços deve conter 1 elemento", function() {
            expect($scope.fornecedor.Enderecos).to.have.length(1);
          });
        });
      });
    });
  });
});