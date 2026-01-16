import { Pipe, PipeTransform } from "@angular/core";

@Pipe({
  name: 'cardDashboardIcon',
  standalone: false
})
export class CardDashboardIconPipe implements PipeTransform {
  transform(tipoDeCard: string): string {

    const cardDashboardIcon: { [key: string]: string } = {
      'Total de Alunos': 'pi pi-graduation-cap',
      'Total de Turmas': 'pi pi-users',
      'Total de Aprovados': 'pi pi-thumbs-up',
      'Total de Reprovados': 'pi pi-thumbs-down'
    }
    return cardDashboardIcon[tipoDeCard];
  }
}