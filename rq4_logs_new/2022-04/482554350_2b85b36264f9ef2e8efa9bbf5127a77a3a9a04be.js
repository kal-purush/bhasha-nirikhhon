import {useState} from 'react';
import './App.css';
import Data from './data';
import {Switch,Route} from 'react-router-dom';
import Detail from './Detail';
import StartPage from './StartPage';
import NavigationBar from './NavigationBar';
import DetailPage from './DetailPage';
import WritePage from './WritePage';
function App() {
  let [article,articleChange]=useState(Data);
  let [viewCnt,viewCntChange] = useState(5);

  return (
    <div className="App">
      {/* 공통페이지 */}
      {/* 내비게이션바 */}

      <NavigationBar/>

      <Switch>
        <Route path="/WritePage">
          <WritePage/>
        </Route>

        {/* 제품별 상세페이지 */}
        <Route path="/Detail/:id">
          <DetailPage article={article}/>
        </Route>

        {/* 상세페이지 */}
        <Route path="/Detail">
          <Detail article={article} viewCnt={viewCnt} viewCntChange={viewCntChange}/>
        </Route>

        {/* 시작페이지 */}
        <Route path="/">
          <StartPage article={article} viewCnt={viewCnt} viewCntChange={viewCntChange}/>
        </Route>
      </Switch>
    </div>
  );
}

export default App;