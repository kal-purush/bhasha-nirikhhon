namespace Banco.Modelos
{
    public class CuentaCheques : CuentaBancaria
    {
        private const decimal TASA_INTERES = 0.02m;
        private const decimal CARGO_CHEQUE = 10m;
        private const decimal CARGO_REBOTADO = 200m;

        private int chequesEmitidos = 0;
        private int chequesRebotados = 0;

        public CuentaCheques(decimal saldoInicial) : base(saldoInicial)
        {
            if (saldoInicial < 5000)
                throw new ArgumentException("El saldo inicial no puede ser menor a $5,000 para una cuenta de cheques.");
        }

        public bool EmitirCheque(decimal cantidad)
        {
            if (cantidad > 0 && cantidad <= this.Saldo)
            {
                this.Saldo -= cantidad;
                chequesEmitidos++;
                return true;
            }
            else
            {
                chequesRebotados++;
                return false;
            }
        }

        public override void RealizarCorteMensual()
        {
            // Aplicar interés
            this.Saldo += this.Saldo * TASA_INTERES;

            // Aplicar cargo por cheques emitidos después del quinto
            if (chequesEmitidos > 5)
            {
                this.Saldo -= (chequesEmitidos - 5) * CARGO_CHEQUE;
            }

            // Aplicar cargo por cheques rebotados
            this.Saldo -= chequesRebotados * CARGO_REBOTADO;

            // Reiniciar contadores
            chequesEmitidos = 0;
            chequesRebotados = 0;
        }
    }
}