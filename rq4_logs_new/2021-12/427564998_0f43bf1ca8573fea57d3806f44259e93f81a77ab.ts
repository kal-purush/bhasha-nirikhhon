import { Component, Input, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { ToastController} from '@ionic/angular';
import { ComportamientoVentas, ComportamientoVentasTotales, DataCredit, FlujoAnual, MonthlyCostFull, OutCome } from 'src/app/models/interfaces';
import { serviceDataBase } from '../../services/services-database';

import { Cuota } from '../../clases/credit'

interface Gestions {
  id: number,
  rank: string
}

@Component({
  selector: 'app-sale-rank',
  templateUrl: './sale-rank.component.html',
  styleUrls: ['./sale-rank.component.scss'],
})
export class SaleRankComponent implements OnInit {

  @Input() idEstim:string;

  public showSpinner:boolean

  public flujoAnual: FlujoAnual = {
    saldoInicial: 0,
    ingresos: 0,
    costoProduccion: 0,
    utilidadBruta: 0,
    costosFijo: 0,
    utilidadNeta: 0,
    cuota: 0,
    flujoAcumulado: 0
  }
  public outCome: OutCome = {
    van: 0,
    tir: "",
    conclusion: "No Factible"
  }
  meses = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']
  totalCostos = 0;
  totalVentas = 0;
  public valueSelected: string = "2021"
  public gestions: Gestions[] = [
    {
      id: 1,
      rank: '2021'
    },
    {
      id: 2,
      rank: '2022',
    },
    {
      id: 3,
      rank: '2023',
    },
    {
      id: 2,
      rank: '2024',
    },
    {
      id: 3,
      rank: '2025',
    }
  ];

  public dataCredit: DataCredit = {
    montoFinanciar: 0,
    tasaInteres: 0,
    plazo: 0, //Solo prueba
    poliza: 0,
    tipoCuota: ""
  }
  public planPagosVariado = new Cuota();
  public cuotas = [];

  public tirCal = 0;
  public tirCalR =0;
  public costoFijoT = 0;

  constructor(
    private router: Router,
    public db: serviceDataBase,
    public toast: ToastController,

  ) {

  }

  goToGraphics(){
    this.navigateTo('annual-flow-graphs');
  }

  ngOnInit() {
    this.idEstim = localStorage.getItem('idEstim')

    //this.getTotalSuma()
    this.showSpinner=true;
    this.getDataCredit()
    this.getFlujoAnual("2021");
    //this.getOutCome();
  }
  navigateTo(path: String) {
    this.router.navigate([path]);
  }

  segmentChanged(event: CustomEvent | any) {
    this.valueSelected = event.detail.value;
    this.getFlujoAnual(this.valueSelected);
  }

  async getFlujoAnual(gestion: string) {
    this.db.getCollection<FlujoAnual>(`/Estimaciones/${this.idEstim}/flujo-anual/${gestion}`).subscribe((data) => {
      if (data == null) {
        if (gestion !== "2021") {
          //this.presentToast("La gestion esta vacia");

        }
        this.flujoAnual = {
          saldoInicial: 0,
          ingresos: 0,
          costoProduccion: 0,
          utilidadBruta: 0,
          costosFijo: 0,
          utilidadNeta: 0,
          cuota: 0,
          flujoAcumulado: 0
        }

      } else {

        this.flujoAnual = data;

        this.flujoAnual.saldoInicial = Math.round(data.saldoInicial)
        this.flujoAnual.costoProduccion = Math.round(data.costoProduccion)
        this.flujoAnual.utilidadBruta = Math.round(data.utilidadBruta)
        this.flujoAnual.utilidadNeta = Math.round(data.utilidadNeta)
        this.flujoAnual.flujoAcumulado = Math.round(data.flujoAcumulado)
      }


    },
      (error: any) => {
        console.log(`Error: ${error}`);
      }
    )
  }

  getOutCome() {
    this.db.getCollection<OutCome>(`/Estimaciones/${this.idEstim}/resultado`).subscribe((data) => {
      this.outCome = data;

    },
      (error: any) => {
        console.log(`Error: ${error}`);
      }
    )
  }


  async presentToast(mensaje: string) {
    const toast = await this.toast.create({
      message: mensaje,
      duration: 2000
    });
    toast.present(); //
  }
  getTotalSuma() {
    this.totalCostos = 0;
    this.totalVentas = 0;

    for (let i = 0; i < this.meses.length; i++) {
      this.db.getCollection<ComportamientoVentas>(`/Estimaciones/${this.idEstim}/comportamientoVentas/` + this.meses[i]).subscribe((data) => {
        if (data !== null) {
          this.totalCostos = this.totalCostos + data.costoVenta;
          this.totalVentas = this.totalVentas + data.venta;
        }
        if (i == 11) {
          let dataTotal = {
            totalVenta: this.totalVentas,
            totalCostoVenta: this.totalCostos,
          }
          this.db.updateData(dataTotal, `/Estimaciones/${this.idEstim}/comportamientoVentas`, 'totales');
          // console.log("----------------------------------------")
          // console.log(this.totalVentas)
          // console.log(this.totalCostos)
        }
      },
        (error: any) => {
          console.log(`Error: ${error}`);

        }
      )
    }

  }

  //Metodos para le Plan de pagos
  getDataCredit() {
    this.db.getCollection<DataCredit>(`/Estimaciones/${this.idEstim}/dato-credito`).subscribe((data) => {
      this.dataCredit = data;
      this.db.getCollection<ComportamientoVentasTotales>(`/Estimaciones/${this.idEstim}/comportamientoVentas/totales`).subscribe((data) => {
        let totalVentaAnual = data.totalVenta;
        let totalCostoVentaAnual = data.totalCostoVenta;
        this.cuotas = []

        if (this.dataCredit.tipoCuota == "Cuota Fija") {

          this.planPagosVariado.calPlanPagosFijo(this.dataCredit.montoFinanciar, this.dataCredit.tasaInteres, this.dataCredit.plazo, this.dataCredit.poliza);
          this.cuotas = this.planPagosVariado.cuotasF
          console.log(this.cuotas)
          this.addFlujoGestion(2021, this.dataCredit.plazo, totalVentaAnual, totalCostoVentaAnual, this.cuotas, this.dataCredit.montoFinanciar, this.dataCredit.tasaInteres, this.dataCredit.plazo);
        }
        if (this.dataCredit.tipoCuota == "Cuota Variable") {
          this.planPagosVariado.calPlanPagosVariado(this.dataCredit.montoFinanciar, this.dataCredit.tasaInteres, this.dataCredit.plazo, this.dataCredit.poliza);
          this.cuotas = this.planPagosVariado.cuotasV
          console.log(this.cuotas)
          this.addFlujoGestion(2021, this.dataCredit.plazo, totalVentaAnual, totalCostoVentaAnual, this.cuotas, this.dataCredit.montoFinanciar, this.dataCredit.tasaInteres, this.dataCredit.plazo);

        }

      },
        (error: any) => {
          console.log(`Error: ${error}`);

        }
      )
    },
      (error: any) => {
        console.log(`Error: ${error}`);

      }
    )
  }

  calCuotaTotal(cuotas: any[], indexInicio: number) {
    let totalCuotaAnual = 0;
    let cont = 0
    for (let i = indexInicio; (cont < 12 && i < cuotas.length); i++) {
      totalCuotaAnual = totalCuotaAnual + cuotas[i]
      cont = cont + 1
    }
    return totalCuotaAnual
  }


  addFlujoGestion(anio: number, numMeses: number, ingresosT: number, costoProducionT: number, cuotas: any[], montoFinanciar: number, tasaInteres: number, plazo: number) {
    let numGestiones = numMeses / 12;
    let saldoInicial = 0;
    let indexCuotas = 0;
    this.db.replaceData({}, `/Estimaciones/${this.idEstim}`, 'flujo-anual')

    this.db.getCollection<MonthlyCostFull>(`/Estimaciones/${this.idEstim}/costos-operativos`).subscribe((data) => {
      let costoFijoT = data.totalCostosOperativos * 12;
      for (let i = 0; i < numGestiones; i++) {
        if (i == 0) {

          this.setFlujoAnual(ingresosT, costoProducionT, saldoInicial, this.calCuotaTotal(cuotas, indexCuotas), costoFijoT);
          //console.log("total Cuota: "+this.calCuotaTotal(this.cuotas, indexCuotas))
          this.db.updateData<FlujoAnual>(this.flujoAnual, `/Estimaciones/${this.idEstim}/flujo-anual`, anio.toString());

          this.outCome.van = this.calVanForFCctte(montoFinanciar, this.flujoAnual.flujoAcumulado, tasaInteres, plazo);
          console.log(this.outCome.van)

          if (this.outCome.van > 0) { this.outCome.conclusion = 'Es Factible' }


          //calcular TIR
          this.generateTirCal(montoFinanciar, this.flujoAnual.flujoAcumulado, plazo);
          this.outCome.tir = this.tirCalR.toFixed(2)
          //carga de datos Reusltado
          this.db.updateData<OutCome>(this.outCome, `/Estimaciones/${this.idEstim}`, 'resultado');

        } else {
          indexCuotas = indexCuotas + 12;
          anio = anio + 1;
          saldoInicial = this.flujoAnual.flujoAcumulado;
          this.setFlujoAnual(ingresosT, costoProducionT, saldoInicial, this.calCuotaTotal(this.cuotas, indexCuotas), costoFijoT);
          this.db.updateData<FlujoAnual>(this.flujoAnual, `/Estimaciones/${this.idEstim}/flujo-anual`, anio.toString());

        }

      }

    },
      (error: any) => {
        console.log(`Error: ${error}`);

      }
    )

  }

  calUtilidadBrutaTotal(totalIngeso: number, totalCostoProduccion: number): number {
    return totalIngeso - totalCostoProduccion;
  }
  calUtilidadNetaTotal(totalUtiliadBruta: number, totalCostoFijo: number): number {
    return totalUtiliadBruta - totalCostoFijo;
  }
  calFlujoAcumuladoTotal(saldoInicial: number, totalUtilidadNeta: number, totalCuota: number): number {
    return totalUtilidadNeta + saldoInicial - totalCuota;
  }

  setFlujoAnual(totalIngreso: number, totalCostoProduccion: number, saldoIncial: number, totalCuota: number, costoFijoT: number) {

    this.flujoAnual.saldoInicial = saldoIncial;
    this.flujoAnual.ingresos = totalIngreso;
    this.flujoAnual.costoProduccion = totalCostoProduccion;
    this.flujoAnual.utilidadBruta = this.calUtilidadBrutaTotal(totalIngreso, totalCostoProduccion);
    this.flujoAnual.costosFijo = costoFijoT;
    this.flujoAnual.utilidadNeta = this.calUtilidadNetaTotal(this.flujoAnual.utilidadBruta, this.flujoAnual.costosFijo);
    this.flujoAnual.cuota = Math.round(totalCuota);
    this.flujoAnual.flujoAcumulado = this.calFlujoAcumuladoTotal(this.flujoAnual.saldoInicial, this.flujoAnual.utilidadNeta, totalCuota);

  }

  //METODOS PARA EL CALCULLO DE VAN (tipo excel)

  calVanForFCctte(inversion: number, fc: number, interes: number, periodo: number): number {

    let expr = Math.pow((1 + (interes / 12) / 100), (-periodo))
    let res = Math.round((fc / 12) * ((1 - expr) / ((interes / 12) / 100)) - inversion)
    return res;
  }

  //METODOS PARA EL CALCULLO DE TIR
  generateTir(k1: number, k2: number, montoFinanciar: number, flujoAcumulado: number, plazo: number): number {

    let van1Posi = false;
    let van2Posi = false;

    let van1 = this.calVanForFCctte(montoFinanciar, flujoAcumulado, k1, plazo);
    let van2 = this.calVanForFCctte(montoFinanciar, flujoAcumulado, k2, plazo);
    console.log("----------------------------------------------");
    console.log("VAN1: " + van1 + " VAN2: " + van2)

    console.log("El mas sercano a cero es TIR: " + this.proximoAcero(k1, k2, van1, van2));
    this.tirCal = (this.proximoAcero(k1, k2, van1, van2));

    if (van1 > 0) {
      van1Posi = true;
    }
    if (van2 > 0) {
      van2Posi = true;
    }

    if (van1Posi == true && van2Posi == false || van2Posi == true && van1Posi == false) {
      let tir = this.interpolar(k1, k2, van1, van2)
      let vanNew = this.calVanForFCctte(montoFinanciar, flujoAcumulado, tir, plazo);

      console.log("nuevo TIR:" + tir + " nuevo VAN: " + vanNew)
      this.tirCalR=tir //Obtecion de TIR

      if (vanNew > 0.0001 && vanNew < 0.9999 || vanNew == 0) {
        this.showSpinner= false;
        return tir;
      } else {
        console.log("K1: " + (k1 - 1) + " K2: " + (k2 + 1))
        if (this.proximoAcero(k1, k2, van1, van2) === k1) {

          //console.log("Nuevo TIR posi:" +this.nuevoTir(k2, tir, van2, vanNew) )
          this.generateTir(k1 - 1, this.nuevoTir(k2, tir, van2, vanNew, montoFinanciar, flujoAcumulado, plazo), montoFinanciar, flujoAcumulado, plazo);
        } else {
          this.generateTir(k1 - 1, this.nuevoTir(k1, tir, van1, vanNew, montoFinanciar, flujoAcumulado, plazo), montoFinanciar, flujoAcumulado, plazo);
        }
        //this.testTir(k1 - 1, k2 + 1);

      }

    } else {
      return 0.000002
    }//

  }

  private interpolar(k1: number, k2: number, van1: number, van2: number) {
    return k1 + (k2 - k1) * (van1 / (van1 - van2))
  }
  private proximoAcero(k1: number, k2: number, van1: number, van2: number): number {
    let van1Compare = van1
    let van2Compare = van2

    if (van1 < 0) {
      van1Compare = van1 * (-1)
    }
    if (van2 < 0) {
      van2Compare = van2 * (-1)
    }
    console.log("n1: " + van1Compare)
    console.log("n2: " + van2Compare)

    if (van1Compare < van2Compare) {
      return k1
    } else {
      return k2
    }
  }

  private nuevoTir(kMay: number, kcal: number, vanMayor: number, vanCal: number, montoFinanciar: number, flujoAcumulado: number, plazo: number): number {
    if (kcal == this.proximoAcero(kcal, kMay, vanCal, vanMayor)) {
      if (vanCal < 0) {

        while (vanCal < 0) {
          //let tir = this.interpolar(k1, k2, van1, van2)

          vanCal = this.calVanForFCctte(montoFinanciar, flujoAcumulado, kcal = kcal - 1, plazo);
        }
        console.log("VAN a positivo: " + vanCal)
        return kcal

      } else {
        return kcal;
      }
    } else {
      return kMay
    }

  }

  generateTirCal(montoFinanciar: number, flujoAcumulado: number, plazo: number) {
    this.generateTir(500, 1, montoFinanciar, flujoAcumulado, plazo);
   //console.log("Este es el tir: " + this.tirCal.toFixed(2));
  }

}