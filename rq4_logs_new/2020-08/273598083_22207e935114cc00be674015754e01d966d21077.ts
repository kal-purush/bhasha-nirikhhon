import { Component, OnInit } from '@angular/core';
import { PedidoService } from 'src/app/services/pedido.service';
import { UserService } from 'src/app/services/user.service';
import Swal, * as Jwal from 'sweetalert2';
import * as jsPDF from 'jspdf';
import autoTable from 'jspdf-autotable';
@Component({
  selector: 'app-pedidos-list',
  templateUrl: './pedidos-list.component.html',
  styleUrls: ['./pedidos-list.component.css']
})
export class PedidosListComponent implements OnInit {

  misPedidos:any;
  clientes:any;
  name:string;
  constructor(private pedidoService:PedidoService, private userService:UserService) { }
  ngOnInit(): void {
    this.cargarPedidos();
    this.cargarUsuarios();
    
  }
  cargarPedidos(){
    this.pedidoService.getPedidos().subscribe(res=>{
      this.misPedidos=res;
      
    });
    document
  }
  cargarUsuarios(){
    this.userService.getUsers().subscribe(res=>{
      this.clientes=res;
      
    });
    
  }
  aprobar(pedido:any){
    this.verifyTime(pedido);
    this.pedidoService.setStatus(pedido._id).subscribe(res=>{ 
      Swal.fire({
        icon:'success',
        text:'La solicitud  ha sido aceptada'
      })     
    });
    this.cargarPedidos();
    
    
  }
  verifyTime(pedido){
    var date=new Date();
    var datePedido=new Date(pedido.createdAt);
    console.log(date, datePedido);
    
  }
  public downloadPDF():void {
    let DATA = document.getElementById('table-pedidos');
    let pdf = new jsPDF({
      orientation:'1',
      unit:'pt',
      format:'a4'
    });
    pdf.text("Listado de Pedidos ",200,30);
    autoTable(pdf, { html: '#table-pedidos' })
    pdf.save('angular-demo.pdf');
  }
}