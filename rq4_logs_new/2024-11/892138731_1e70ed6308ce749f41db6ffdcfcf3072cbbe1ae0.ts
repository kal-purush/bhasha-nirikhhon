export function formatDate(date: Date): string {
  const daysOfWeek = [
    'Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira',
    'Quinta-feira', 'Sexta-feira', 'Sábado'
  ];

  const months = [
    'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
    'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
  ];

  const dayOfWeek = daysOfWeek[date.getDay()];
  const day = date.getDate();
  const month = months[date.getMonth()];
  const hour = date.getHours();
  const minutes = date.getMinutes();

  // Formatar minutos para sempre ter dois dígitos
  const formattedMinutes = minutes < 10 ? `0${minutes}` : minutes;

  // Formatar horas no formato 12h com sufixo am/pm
  const hour12Format = hour % 12 || 12; // Transforma 0 em 12
  const period = hour < 12 ? 'am' : 'pm';

  return `${dayOfWeek}, ${day} de ${month} às ${hour12Format}:${formattedMinutes}${period}`;
}