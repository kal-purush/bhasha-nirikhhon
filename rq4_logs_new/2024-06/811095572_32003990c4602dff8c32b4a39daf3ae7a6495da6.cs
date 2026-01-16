namespace Banco.Modelos
{
    public abstract class CuentaBancaria
    {
        private decimal saldo;

        public decimal Saldo
        {
            get { return saldo; }
            protected set { saldo = value; }
        }

        public CuentaBancaria(decimal saldoInicial)
        {
            this.saldo = saldoInicial;
        }

        public void Depositar(decimal cantidad)
        {
            if (cantidad > 0)
            {
                this.saldo += cantidad;
            }
        }

        public virtual bool Retirar(decimal cantidad)
        {
            if (cantidad > 0 && cantidad <= this.saldo)
            {
                this.saldo -= cantidad;
                return true;
            }
            return false;
        }

        public abstract void RealizarCorteMensual();
    }
}
