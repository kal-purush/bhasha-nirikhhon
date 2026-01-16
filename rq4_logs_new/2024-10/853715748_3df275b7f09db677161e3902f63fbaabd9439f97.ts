import { createStore } from 'vuex';
import DoraData from './DoraData';
import CopilotUsage from './CopilotUsage';

export default createStore({
  modules: {
    DoraData,
    CopilotUsage,
  },
});