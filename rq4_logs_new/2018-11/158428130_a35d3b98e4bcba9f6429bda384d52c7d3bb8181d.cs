using Entidades;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;
using System.Threading.Tasks;

namespace Logica
{
    [ServiceContract]
    public interface IProductoBL
    {
        [OperationContract]
        [Description("Servicio REST que permite registrar productos")]
        [WebInvoke(RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, Method = "POST", UriTemplate = "/RegistrarProducto", BodyStyle = WebMessageBodyStyle.Bare)]
        int NuevoProducto(ProductoE producto);

        [OperationContract]
        int EditarProducto(ProductoE producto);

        [OperationContract]
        int EliminarProducto(int idProducto);

        [OperationContract]
        ProductoE BuscarProducto(int idProducto);

        [OperationContract]
        [Description("Servicio REST que permite listar todos Productos")]
        [WebGet(RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, UriTemplate = "/ListarProductos", BodyStyle = WebMessageBodyStyle.Bare)]
        List<ProductoE> MostrarProductos(bool todos);

        [OperationContract]
        [Description("Servicio REST que permite listar todos Productos")]
        [WebGet(RequestFormat = WebMessageFormat.Json, ResponseFormat = WebMessageFormat.Json, UriTemplate = "/ListarTipos", BodyStyle = WebMessageBodyStyle.Bare)]
        IEnumerable<TipoProductoE> MostrarTipos();

    }
}