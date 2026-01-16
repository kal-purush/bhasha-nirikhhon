using lib_entidades;
using lib_entidades.Modelos;
using lib_presentaciones.Interfaces;
using lib_utilidades;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace asp_presentaciones.Pages.Ventanas
{
    public class EstudiantesModel : PageModel
    {
        private IEstudiantesPresentacion? iPresentacion = null;

        public EstudiantesModel(IEstudiantesPresentacion iPresentacion)
        {
            try
            {
                this.iPresentacion = iPresentacion;
                Filtro = new Estudiantes();
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public IFormFile? FormFile { get; set; }
        [BindProperty] public Enumerables.Ventanas Accion { get; set; }
        [BindProperty] public Estudiantes? Actual { get; set; }
        [BindProperty] public Estudiantes? Filtro { get; set; }
        [BindProperty] public List<Estudiantes>? Lista { get; set; }
        [BindProperty] public List<Niveles>? Niveles { get; set; } //Lista de claves foraneas
        [BindProperty] public List<Estados>? Estados { get; set; } //Lista de claves foraneas

        public virtual void OnGet() { OnPostBtRefrescar(); }

        public void OnPostBtRefrescar()
        {
            try
            {
                var variable_session = HttpContext.Session.GetString("key");
                if (String.IsNullOrEmpty(variable_session))
                    HttpContext.Session.SetString("key", "Pruebas");

                Filtro!.Nombre = Filtro!.Nombre ?? "";

                Accion = Enumerables.Ventanas.Listas;
                var task = this.iPresentacion!.Buscar(Filtro!, "NOMBRE");
                task.Wait();
                Lista = task.Result;
                CargarCombox();
                Actual = null;
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public virtual void OnPostBtNuevo()
        {
            try
            {
                Accion = Enumerables.Ventanas.Editar;
                CargarCombox();
                Actual = new Estudiantes()
                {
                    //Fecha = DateTime.Now,
                };
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public virtual void OnPostBtModificar(string data)
        {
            try
            {
                OnPostBtRefrescar();
                Accion = Enumerables.Ventanas.Editar;
                Actual = Lista!.FirstOrDefault(x => x.Id.ToString() == data);
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public virtual void OnPostBtGuardar()
        {
            try
            {
                Accion = Enumerables.Ventanas.Editar;
                /*if (FormFile != null)
                {
                    var memoryStream = new MemoryStream();
                    FormFile.CopyToAsync(memoryStream).Wait();
                    Actual!.Imagen = EncodingHelper.ToString(memoryStream.ToArray());
                    memoryStream.Dispose();
                }*/ //para imagen 

                Task<Estudiantes>? task = null;
                if (Actual!.Id == 0)
                    task = this.iPresentacion!.Guardar(Actual!);
                else
                    task = this.iPresentacion!.Modificar(Actual!);
                task.Wait();
                Actual = task.Result;
                Accion = Enumerables.Ventanas.Listas;
                OnPostBtRefrescar();
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public virtual void OnPostBtBorrarVal(string data)
        {
            try
            {
                OnPostBtRefrescar();
                Accion = Enumerables.Ventanas.Borrar;
                Actual = Lista!.FirstOrDefault(x => x.Id.ToString() == data);
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public virtual void OnPostBtBorrar()
        {
            try
            {
                var task = this.iPresentacion!.Borrar(Actual!);
                Actual = task.Result;
                OnPostBtRefrescar();
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public void OnPostBtCancelar()
        {
            try
            {
                Accion = Enumerables.Ventanas.Listas;
                OnPostBtRefrescar();
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public void OnPostBtCerrar()
        {
            try
            {
                if (Accion == Enumerables.Ventanas.Listas)
                    OnPostBtRefrescar();
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        //Para claves foraneas
        public void CargarCombox()
        {
            try
            {
                if (!(Niveles == null || Niveles!.Count <= 0))
                    return;

                Niveles = new List<Niveles>()
                {
                    new Niveles() { Id = 0, Nombre = " " },
                    new Niveles() { Id = 1, Nombre = "Semestre 1" },
                    new Niveles() { Id = 2, Nombre = "Semestre 2" },
                    new Niveles() { Id = 3, Nombre = "Semestre 3" },
                    new Niveles() { Id = 4, Nombre = "Semestre 4" },
                    new Niveles() { Id = 5, Nombre = "Semestre 5" },
                    new Niveles() { Id = 6, Nombre = "Semestre 6" },
                    new Niveles() { Id = 7, Nombre = "Semestre 7" },
                    new Niveles() { Id = 8, Nombre = "Semestre 8" },
                    new Niveles() { Id = 9, Nombre = "Semestre 9" },
                    new Niveles() { Id = 10, Nombre = "Semestre 10" },

                };

                if (!(Estados == null || Estados!.Count <= 0))
                    return;

                Estados = new List<Estados>()
                {
                    new Estados() { Id = 0, Nombre = " " },
                    new Estados() { Id = 1, Nombre = "Activo" },
                    new Estados() { Id = 2, Nombre = "Inactivo" },

                };
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
            }
        }

        public string ConvertirNivel(int id)
        {
            try
            {
                CargarCombox();
                return Niveles!.FirstOrDefault(x => x.Id == id)!.Nombre!;
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
                return string.Empty;
            }
        }

        public string ConvertirEstado(int id)
        {
            try
            {
                CargarCombox();
                return Estados!.FirstOrDefault(x => x.Id == id)!.Nombre!;
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
                return string.Empty;
            }
        }

        /*public string ConvertirActivo(bool valor)
        {
            try
            {
                return valor ? "Activo" : "Inactivo";
            }
            catch (Exception ex)
            {
                LogConversor.Log(ex, ViewData!);
                return "Falso";
            }
        }*/
    }
}