            $(function(){
                CarregarPedidos();
            }());

            function CarregarPedidos(){
                $("#pedidos").load('http://187.255.165.251:8080/iComidaREST/rest/icomida/pedidos', function(){
                   HabilitarListeners();
                });
            }

            function HabilitarListeners()
            {
                $("#pedidos table tr").each(function(){
                    var _self = $(this);
                    _self.find("a").on("click", function(e){
			e.preventDefault();
                        var idPedido = _self.find("td:first").text();
                        AtualizarPedidos(idPedido, 4);
                    });
                });
            }

            function AtualizarPedidos(pedido, status){
                $.ajax({
                    type: 'PUT',
                    crossDomain: true,
                    url: 'http://187.255.165.251:8080/iComidaREST/rest/icomida/alterarPedidoitem/' + pedido + '/' + status,
                    success: function(data){
                        CarregarPedidos();
                    }
                });
            }