namespace Banco.Modelos
{
    public abstract class CuentaBancaria : ICuentaBancaria
    {
        public decimal Saldo { get; protected set; }

        public CuentaBancaria(decimal saldoInicial)
        {
            Saldo = saldoInicial;
        }

        public void Depositar(decimal cantidad)
        {
            if (cantidad > 0)
            {
                Saldo += cantidad;
            }
        }

        public bool Retirar(decimal cantidad)
        {
            if (cantidad > 0 && cantidad <= Saldo)
            {
                Saldo -= cantidad;
                return true;
            }
            return false;
        }

        public abstract void RealizarCorteMensual();
    }
}