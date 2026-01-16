import { BrowserRouter } from "react-router-dom";
import AppRoute from "./routes/routes";
import GContext from "./Context";
function App() {
  return (
    <BrowserRouter>
      <GContext>
        <AppRoute />
      </GContext>
    </BrowserRouter>
  );
}

export default App;