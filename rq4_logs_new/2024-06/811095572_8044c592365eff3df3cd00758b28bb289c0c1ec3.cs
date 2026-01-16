namespace Banco.Modelos
{
    public interface ICuentaBancaria
    {
        decimal Saldo { get; }
        void Depositar(decimal cantidad);
        bool Retirar(decimal cantidad);
        void RealizarCorteMensual();
    }
}
