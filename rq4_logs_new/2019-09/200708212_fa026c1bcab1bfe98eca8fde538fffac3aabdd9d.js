import Statistic from '../components/statistic.js';
import {
  addElementDOM,
  removeContainerChildren
} from '../utils.js';
import {
  userTotalRating,
  getWatchedFilmsAmount,
  getTotalDuration,
  getTopGenre,
  statisticFiltersId
} from '../data.js';

/**
 * Class representaing controller of statistic.
 */
class StatisticController {
  /**
   * Create statistic controller.
   * @param {HTMLElement} statisticContainer
   */
  constructor(statisticContainer) {
    this._statisticContainer = statisticContainer;
    this._statisticComponent = null;

    this.onUpdateStatistic = this.onUpdateStatistic.bind(this);
  }

  /**
   * Create statistic.
   */
  init() {
    this._addStatistic(
        this._getStatisticParams(statisticFiltersId.allTime),
        statisticFiltersId.allTime);
  }

  /**
   * Add statistic to DOM and fill handlers.
   * @param {object} statisticParams
   * @param {string} filter
   */
  _addStatistic(statisticParams, filter) {
    this._statisticComponent = new Statistic(this._statisticContainer,
        userTotalRating, statisticParams, filter, this.onUpdateStatistic);
    addElementDOM(this._statisticContainer, this._statisticComponent);
    this._statisticComponent.renderChart();
  }

  /**
   * Filter statistic.
   * @param {string} newFilter
   */
  onUpdateStatistic(newFilter) {
    removeContainerChildren(this._statisticContainer);
    this._statisticComponent.unrender();
    this._addStatistic(this._getStatisticParams(newFilter), newFilter);
  }

  /**
   * Return statistic params.
   * @return {object}
   */
  _getStatisticParams() {
    return {
      totalWatchedFilms: getWatchedFilmsAmount(),
      totalDuration: getTotalDuration(),
      topGenre: getTopGenre()
    };
  }
}

export default StatisticController;