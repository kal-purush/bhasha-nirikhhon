import React from "react";
import {
  PortfolioWrapper,
  PortfolioItemWrapper,
  PortfolioImageWrapper,
  PortfolioTextWrapper,
  PortfolioGridWrapper,
} from "./Portfolio.style";
import { ItemSep, ItemTech } from "../History/History.style";
import { SectionHeader } from "../../globals/styled";

import AnimateIn from "../AnimateIn/AnimateIn";

import SpendingInsights from "../../assets/spending-insights.png";
import GamedayLive from "../../assets/mlb-gameday-live.png";
import GamedayWrap from "../../assets/mlb-gameday-wrap.png";
import Schedule from "../../assets/mlb-schedule.png";
import Scores from "../../assets/mlb-scores.png";
import StatsPage from "../../assets/mlb-stats.png";

const portfolioConfig = [
  {
    title: "Spending Insights",
    tech: ["Angular", "D3.js", "SCSS"],
    imgSrc: SpendingInsights,
  },
  {
    title: "MLB Gameday Live",
    tech: ["React", "Redux", "styled-components"],
    imgSrc: GamedayLive,
  },
  {
    title: "MLB Gameday Wrap",
    tech: ["React", "Redux", "styled-components"],
    imgSrc: GamedayWrap,
  },
  {
    title: "MLB Scores",
    tech: ["React", "Redux", "styled-components"],
    imgSrc: Scores,
  },
  {
    title: "MLB Schedule",
    tech: ["React", "Redux", "styled-components"],
    imgSrc: Schedule,
  },
  {
    title: "MLB Stats",
    tech: ["React", "Redux", "SCSS"],
    imgSrc: StatsPage,
  },
];

const Portfolio = () => {
  return (
    <PortfolioWrapper id="portfolio">
      <AnimateIn>
        <SectionHeader>Portfolio</SectionHeader>
      </AnimateIn>
      <PortfolioGridWrapper>
        {portfolioConfig.map((config) => {
          const { title, tech, imgSrc } = config;
          return (
            <AnimateIn key={title}>
              <PortfolioItemWrapper>
                <PortfolioImageWrapper>
                  <img src={imgSrc} />
                </PortfolioImageWrapper>
                <PortfolioTextWrapper>
                  <h2>{title}</h2>
                  <div>
                    {tech.map((t, i) => {
                      return (
                        <React.Fragment key={i}>
                          <ItemTech>{t}</ItemTech>
                          {i < tech.length - 1 ? <ItemSep>|</ItemSep> : null}
                        </React.Fragment>
                      );
                    })}
                  </div>
                </PortfolioTextWrapper>
              </PortfolioItemWrapper>
            </AnimateIn>
          );
        })}
      </PortfolioGridWrapper>
    </PortfolioWrapper>
  );
};

export default Portfolio;