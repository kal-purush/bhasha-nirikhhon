export class HoraFuncionamentoValidator {

  private inicio: string;
  private horaInicio: string | number;
  private minutoInicio: string | number;

  private termino: string;
  private horaTermino: string | number;
  private minutoTermino: string | number;

  constructor(inicio: string = '', termino: string = '') {
    this.inicio = inicio ?? '0';
    this.termino = termino ?? '0';

    this.horaInicio = Number(this.inicio.split(':')[0]);
    this.minutoInicio = Number(this.inicio.split(':')[1]);

    this.horaTermino = Number(this.termino.split(':')[0]);
    this.minutoTermino = Number(this.termino.split(':')[1]);
  }

  validaLimiteTempo() {

    if (this.horaInicio > this.horaTermino)
      throw new Error('Hora de início é maior do que a hora de termino!');

    if (this.horaInicio === this.horaTermino) {
      if (this.minutoTermino < (Number(this.minutoInicio) + 15)) //Alterar para uma constante
        throw new Error('O Limite minimo é de 15 minutos entre os horários!');
    }

    return this;
  }

  validaHoraMinuto() {
    if (!this.horaInicio || this.horaInicio === NaN || this.minutoInicio === NaN)
      throw new Error('Hora de início informada invalida!');

    if (!this.horaTermino ||  this.horaTermino === NaN|| this.minutoTermino === NaN)
      throw new Error('Hora de termino informada invalida!');

    if (this.horaInicio > 23 || this.horaTermino > 23)
      throw new Error('Hora informada invalida!');

    if (this.minutoInicio > 59 || this.minutoTermino > 59)
      throw new Error('Minuto informado invalido!');

    return this;
  }

}