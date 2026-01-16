namespace Banco.Modelos
{
    public class CuentaAhorro : CuentaBancaria
    {
        private const decimal TASA_INTERES = 0.01m;
        private const decimal COMISION = 50m;

        public CuentaAhorro(decimal saldoInicial) : base(saldoInicial)
        {
            if (saldoInicial < 3000)
                throw new ArgumentException("El saldo inicial no puede ser menor a $3,000 para una cuenta de ahorro.");
        }

        public override void RealizarCorteMensual()
        {
            // Aplicar interés
            this.Saldo += this.Saldo * TASA_INTERES;

            // Aplicar comisión si el saldo es inferior a $1,000
            if (this.Saldo < 1000)
            {
                this.Saldo -= COMISION;
            }
        }
    }
}