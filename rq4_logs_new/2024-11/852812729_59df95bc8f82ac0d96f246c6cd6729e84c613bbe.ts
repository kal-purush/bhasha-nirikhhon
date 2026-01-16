import { Component } from '@angular/core';

@Component({
  selector: 'app-team-page',
  templateUrl: './team-page.component.html',
  styleUrl: './team-page.component.scss'
})
export class TeamPageComponent {

  team_description=[
    {
      title: 'CODATA é a coordenadoria de análise de dados da Prefeitura de São Paulo.', 
      txt_0: 'Lorem ipsum dolor sit amet consectetur adipisicing elit. Voluptates totam sit tenetur, non ut voluptatum ab dolor at minima dicta amet itaque delectus quasi iure ducimus autem dignissimos odio! Sapiente.', 
      txt_1: 'Lorem ipsum dolor sit amet consectetur adipisicing elit. Voluptates totam sit tenetur, non ut voluptatum ab dolor at minima dicta amet itaque delectus quasi iure ducimus autem dignissimos odio! Sapiente.', 
      txt_2: 'Lorem ipsum dolor sit amet consectetur adipisicing elit. Voluptates totam sit tenetur, non ut voluptatum ab dolor at minima dicta amet itaque delectus quasi iure ducimus autem dignissimos odio! Sapiente.',
      txt_3: 'Lorem ipsum dolor sit amet consectetur adipisicing elit. Voluptates totam sit tenetur.',
    }
  ];

  image_team=[
    {
      img:'https://media2.dayoftheshirt.com/7jhot5tsp9wx6oofgougyg3c0ncy/psicopato-pampling.jpg', 
      complete_name:'Henrique Pougy', 
      position:'Coordenador', 
      description_position:' Suspendisse aliquam vulputate turpis, vitae dapibus orci bibendum at. Quisque ut imperdiet velit. Ut molestie ante turpis. Vivamus ligula lectus, malesuada et sapien nec, euismod efficitur nisi. Sed erat felis, efficitur a ante vel, facilisis consequat nisi. Aenean efficitur tincidunt mauris eget rhoncus. Praesent sapien tortor, posuere a dictum sit amet, rhoncus quis mauris. Phasellus consectetur nisl a enim tincidunt volutpat. Praesent vehicula ultricies tortor ut commodo. Donec bibendum dui in felis faucibus, eget pellentesque odio tincidunt. Fusce dictum'
    },
    {
      img:'https://media2.dayoftheshirt.com/7jhot5tsp9wx6oofgougyg3c0ncy/psicopato-pampling.jpg', 
      complete_name:'Bruno Azuma', 
      position:'Analista de Planejamento e Desenvolvimento Organizacional', 
      description_position:' Suspendisse aliquam vulputate turpis, vitae dapibus orci bibendum at. Quisque ut imperdiet velit. Ut molestie ante turpis. Vivamus ligula lectus, malesuada et sapien nec, euismod efficitur nisi. Sed erat felis, efficitur a ante vel, facilisis consequat nisi. Aenean efficitur tincidunt mauris eget rhoncus. Praesent sapien tortor, posuere a dictum sit amet, rhoncus quis mauris. Phasellus consectetur nisl a enim tincidunt volutpat. Praesent vehicula ultricies tortor ut commodo. Donec bibendum dui in felis faucibus, eget pellentesque odio tincidunt. Fusce dictum'
    },
    {
      img:'https://media2.dayoftheshirt.com/7jhot5tsp9wx6oofgougyg3c0ncy/psicopato-pampling.jpg', 
      complete_name:'Mauryas Manzoli', 
      position:'Arquiteto', 
      description_position:' Suspendisse aliquam vulputate turpis, vitae dapibus orci bibendum at. Quisque ut imperdiet velit. Ut molestie ante turpis. Vivamus ligula lectus, malesuada et sapien nec, euismod efficitur nisi. Sed erat felis, efficitur a ante vel, facilisis consequat nisi. Aenean efficitur tincidunt mauris eget rhoncus. Praesent sapien tortor, posuere a dictum sit amet, rhoncus quis mauris. Phasellus consectetur nisl a enim tincidunt volutpat. Praesent vehicula ultricies tortor ut commodo. Donec bibendum dui in felis faucibus, eget pellentesque odio tincidunt. Fusce dictum'
    },
    {
      img:'https://media2.dayoftheshirt.com/7jhot5tsp9wx6oofgougyg3c0ncy/psicopato-pampling.jpg', 
      complete_name:'David Silva', 
      position:'Assistente Administrativo de Gestão', 
      description_position:' Suspendisse aliquam vulputate turpis, vitae dapibus orci bibendum at. Quisque ut imperdiet velit. Ut molestie ante turpis. Vivamus ligula lectus, malesuada et sapien nec, euismod efficitur nisi. Sed erat felis, efficitur a ante vel, facilisis consequat nisi. Aenean efficitur tincidunt mauris eget rhoncus. Praesent sapien tortor, posuere a dictum sit amet, rhoncus quis mauris. Phasellus consectetur nisl a enim tincidunt volutpat. Praesent vehicula ultricies tortor ut commodo. Donec bibendum dui in felis faucibus, eget pellentesque odio tincidunt. Fusce dictum'
    },
    {
      img:'https://cdn.awsli.com.br/2021/2021727/produto/270410343/pata1-3vnslxwa1c.jpg', 
      complete_name:'Ana Carolina', 
      position:'Front-End Developer', 
      description_position:' Suspendisse aliquam vulputate turpis, vitae dapibus orci bibendum at. Quisque ut imperdiet velit. Ut molestie ante turpis. Vivamus ligula lectus, malesuada et sapien nec, euismod efficitur nisi. Sed erat felis, efficitur a ante vel, facilisis consequat nisi. Aenean efficitur tincidunt mauris eget rhoncus. Praesent sapien tortor, posuere a dictum sit amet, rhoncus quis mauris. Phasellus consectetur nisl a enim tincidunt volutpat. Praesent vehicula ultricies tortor ut commodo. Donec bibendum dui in felis faucibus, eget pellentesque odio tincidunt. Fusce dictum'
    },
    {
      img:'https://media2.dayoftheshirt.com/7jhot5tsp9wx6oofgougyg3c0ncy/psicopato-pampling.jpg', 
      complete_name:'Pedro Daré', 
      position:'Design UI/UX', 
      description_position:' Suspendisse aliquam vulputate turpis, vitae dapibus orci bibendum at. Quisque ut imperdiet velit. Ut molestie ante turpis. Vivamus ligula lectus, malesuada et sapien nec, euismod efficitur nisi. Sed erat felis, efficitur a ante vel, facilisis consequat nisi. Aenean efficitur tincidunt mauris eget rhoncus. Praesent sapien tortor, posuere a dictum sit amet, rhoncus quis mauris. Phasellus consectetur nisl a enim tincidunt volutpat. Praesent vehicula ultricies tortor ut commodo. Donec bibendum dui in felis faucibus, eget pellentesque odio tincidunt. Fusce dictum'
    },
    {
      img:'https://cdn.awsli.com.br/2021/2021727/produto/270410343/pata1-3vnslxwa1c.jpg', 
      complete_name:'Olivia Amann', 
      position:'Analista de Dados', 
      description_position:' Suspendisse aliquam vulputate turpis, vitae dapibus orci bibendum at. Quisque ut imperdiet velit. Ut molestie ante turpis. Vivamus ligula lectus, malesuada et sapien nec, euismod efficitur nisi. Sed erat felis, efficitur a ante vel, facilisis consequat nisi. Aenean efficitur tincidunt mauris eget rhoncus. Praesent sapien tortor, posuere a dictum sit amet, rhoncus quis mauris. Phasellus consectetur nisl a enim tincidunt volutpat. Praesent vehicula ultricies tortor ut commodo. Donec bibendum dui in felis faucibus, eget pellentesque odio tincidunt. Fusce dictum'
    },
  ];

  isPopupOpen = false; 
  selectedTeam: any = null;
  popupAnimationClass: string = '';

  openPopup(team: any): void {
    this.selectedTeam = team; 
    this.popupAnimationClass = 'slide-top'; 
    this.isPopupOpen = true; 
  }

  closePopup(): void {
    this.popupAnimationClass = 'slide-bottom';
    setTimeout(() => {
      this.isPopupOpen = false;
    }, 500); 
  }
}