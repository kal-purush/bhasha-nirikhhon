import { shallow } from "enzyme";
import GridCell from "./grid-cell.component";

it("doesn't explode", () => {
  expect(shallow(<GridCell isAlive={true} isLiveGame={true} handleCellClick={() => {}} />)).toMatchSnapshot();
});