// See https://aka.ms/new-console-template for more information
using System;
using Hospital.App.Persistencia;
using Hospital.App.Dominio;

public class Program
{
    private static IRepositorioMedico _repositorioMedico = new RepositorioMedico (new Hospital.App.Persistencia.AppContext());
    private static IRepositorioPersona _repositorioPersona = new RepositorioPersona (new Hospital.App.Persistencia.AppContext());
    private static IRepositorioPaciente _repositorioPaciente = new RepositorioPaciente (new Hospital.App.Persistencia.AppContext());




    private static  void Main (string[] args)
    {
        Console.WriteLine("Hello, world");

        AddMedico();
        //AddPaciente();
        //BuscarMedico();
        //medicoBorrado();
        //medicoActualizado();
        //listadoMedicos();
        //listadoPersona();
        //listadoPaciente();
    }

    public static void AddMedico()
    {
        var medico = new Medico();
        // Datos Personas
        medico.Especialidad="corred";
        medico.Codigo="441231";
        medico.RegistroRethus="asd4356";
        // Datos Médicos
        medico.Nombre="Roberto";
        medico.Apellidos="Camiels";
        medico.NumeroTelefono="65498";
        medico.Genero="m";

        _repositorioMedico.AddMedico(medico);
        Console.WriteLine("Medico adicionado");
    }
    public static void AddPaciente()
    {
        var paciente = new Paciente();
        // Datos Personas
        paciente.Direccion="calle3";
        paciente.Latitud=441231;
        paciente.Longitud=4568;
        paciente.Ciudad="441231";
        paciente.FechaNacimiento=new DateTime(2021,09,15);
        paciente.MedicoId=1;
        // Datos Médicos
        paciente.Nombre="Roberto";
        paciente.Apellidos="Camiels";
        paciente.NumeroTelefono="65498";
        paciente.Genero="m";

        _repositorioPaciente.AddPaciente(paciente);
        Console.WriteLine("Paciente adicionado");
    }

    public static void BuscarMedico()
    {
        Console.WriteLine("**************");
        Console.WriteLine("Buscando Médico...");
        var medico = _repositorioMedico.GetMedico(5);
        Console.WriteLine(medico.Codigo);
    }

    public static void listadoMedicos()
    {
        var medicos = _repositorioMedico.GetAllMedicos();
        foreach(var m in medicos)
        {
            Console.WriteLine("ID: "+ m.Id +" Código: "+ m.Codigo);
        }
    }   

    public static void listadoPersona()
    {
        var personas = _repositorioPersona.GetAllPersonas();
        foreach(var p in personas)
        {
            Console.WriteLine("ID: "+ p.Id +" Nombre: "+ p.Nombre);
        }
    } 
    public static void listadoPaciente()
    {
        var pacientes = _repositorioPaciente.GetAllPacientes();
        foreach(var p in pacientes)
        {
            Console.WriteLine("ID: "+ p.Id +" Nombre: "+ p.Nombre);
        }
    } 
    public static void medicoBorrado()
    {        
        Console.WriteLine("**************");
        Console.WriteLine("Buscando médico para eliminarlo...");
        _repositorioMedico.DeleteMedico(5);
        Console.WriteLine("Médico eliminado");
    }

    public static void medicoActualizado()
    {        
        var medico = new Medico();
        // Buscar objeto
        medico.Id = _repositorioMedico.GetMedico(8).Id;
        Console.WriteLine("Id Encontrado: "+medico.Id);
        
        medico.Especialidad="nutricionista";
        medico.Codigo="9512";
        medico.RegistroRethus="zx987";
        // Datos Médicos
        medico.Nombre="David";
        medico.Apellidos="Delgado";
        medico.NumeroTelefono="965741";
        medico.Genero="m";
        Console.WriteLine("**************");
        var medicoEncontrado=_repositorioMedico.UpdateMedico(medico);
        Console.WriteLine("Médico actualizado: "+medicoEncontrado.Codigo);
    }
}