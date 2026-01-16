import { Component, OnInit } from '@angular/core';
import { faChartLine } from '@fortawesome/free-solid-svg-icons';

@Component({
  selector: 'app-contribs',
  templateUrl: './contribs.component.html',
  styleUrls: ['./contribs.component.scss']
})
export class ContribsComponent implements OnInit {

  logoForecastHub = faChartLine;
  contribs = [
    { name: 'Covid Analytics, MIT Operations Research Center', method: 'Modified SEIR compartmental model', link: 'https://www.covidanalytics.io/' },
    { name: 'Epiforecasts / London School of Hygiene and Tropical Medicine', method: 'tba', link: 'https://epiforecasts.io/'},
    { name: 'Faculty of Mathematics, Informatics and Mechanics, University of Warsaw', method: 'tba', link: 'https://www.mimuw.edu.pl/en/faculty'},
    { name: 'Frankfurt Institute for Advanced Studies & Forschungszentrum Jülich', method: 'Extended SEIR compartmental model', link: 'https://www.medrxiv.org/content/10.1101/2020.04.18.20069955v1'},
    { name: 'IMISE/GenStat, University of Leipzig', method: 'SECIR compartmental model', link: 'https://www.imise.uni-leipzig.de/en/homepage'},
    { name: 'Institute of Global Health, University of Geneva / Swiss Data Science Center', method: 'Ensemble approach based on estimates of reproductive numbers', link: 'https://renkulab.shinyapps.io/COVID-19-Epidemic-Forecasting/'},
    { name: 'Institute of Health Metrics and Evaluation (IHME), University of Washington', method: 'Hybrid of statistical and disease transmission model', link: 'https://covid19.healthdata.org/'},
    { name: 'Institute of Stochastics, Technical University of Ilmenau', method: 'Simulation approach based on regional estimates of the reproductive number', link: 'https://www.tu-ilmenau.de/stochastik/'},
    { name: 'Interdisciplinary Centre for Mathematical and Computational Modelling (ICM), University of Warsaw', method: 'Agent-based microsimulation model', link: 'https://icm.edu.pl/en/'},
    { name: 'Johannes Gutenberg University Mainz / University of Hamburg', method: 'Statistical dynamical growth model accounting for population susceptibility', link: 'https://github.com/QEDHamburg/covid19'},
    { name: 'Los Alamos National Laboratory (LANL)', method: 'Dynamic growth rate approach', link: 'https://covid-19.bsvgateway.org/'},
    { name: 'MOCOS Group, University of Wrozlaw', method: 'Agent-based microsimulation model', link: 'https://mocos.pl/'},
    { name: 'MRC Centre for Global Infectious Disease Analysis, Imperial College London', method: 'Ensemble of four statistical / disease transmission models', link: 'https://mrc-ide.github.io/covid19-short-term-forecasts/'},
    { name: 'UCLA Statistical Machine Learning Lab', method: 'SuEIR compartmental model', link: 'https://covid19.uclaml.org/'},
    { name: 'University of Southern California Data Science Lab', method: 'SI-kJ alpha disease transmission model', link: 'https://scc-usc.github.io/ReCOVER-COVID-19/'},
    { name: 'Youyang Gu', method: 'SEIR disease transmission model with machine learning layer', link: 'https://covid19-projections.com/'},
  ]

  constructor() { }

  ngOnInit(): void {
  }

}