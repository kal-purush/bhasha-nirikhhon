import React from "react";
import {
  OptionList,
  Heading,
  Card,
  Label,
  Select,
  Tabs
} from "@shopify/polaris";
import "./customStyle.css";
export default class TabsExample extends React.Component {
  state = {
    selected: 0,
    selectedOptions: []
  };

  handleTabChange = selectedTabIndex => {
    this.setState({ selected: selectedTabIndex });
  };

  render() {
    const { selected } = this.state;
    const tabs = [
      {
        id: "repeat-customers",
        content: "Collections",
        panelID: "repeat-customers-content"
      },
      {
        id: "prospects",
        content: "Snapshots",
        panelID: "prospects-content"
      }
    ];

    return (
      <Card>
        <Heading style={{ color: "red" }}>Snapshot by Branded Online</Heading>
        <Tabs tabs={tabs} selected={selected} onSelect={this.handleTabChange}>
          <Card.Section>
            <OptionListExample />
          </Card.Section>
        </Tabs>
      </Card>
    );
  }
}

class OptionListExample extends React.Component {
  state = { selected: [] };
  render() {
    return (
      <div className="cMenu">
        <div class="Polaris-Card">
          <ul class="Polaris-OptionList">
            <li>
              <p class="Polaris-OptionList__Title">COLLECTIONS</p>
              <ul class="Polaris-OptionList__Options" id="OptionList1">
                <li class="Polaris-OptionList-Option" tabindex="-1">
                  <button
                    id="OptionList1-0-0"
                    type="button"
                    class="Polaris-OptionList-Option__SingleSelectOption"
                  >
                    Byward Market
                  </button>
                </li>
                <li class="Polaris-OptionList-Option" tabindex="-1">
                  <button
                    id="OptionList1-0-1"
                    type="button"
                    class="Polaris-OptionList-Option__SingleSelectOption"
                  >
                    Centretown
                  </button>
                </li>
                <li class="Polaris-OptionList-Option" tabindex="-1">
                  <button
                    id="OptionList1-0-2"
                    type="button"
                    class="Polaris-OptionList-Option__SingleSelectOption"
                  >
                    Hintonburg
                  </button>
                </li>
                <li class="Polaris-OptionList-Option" tabindex="-1">
                  <button
                    id="OptionList1-0-3"
                    type="button"
                    class="Polaris-OptionList-Option__SingleSelectOption"
                  >
                    Westboro
                  </button>
                </li>
                <li class="Polaris-OptionList-Option" tabindex="-1">
                  <button
                    id="OptionList1-0-4"
                    type="button"
                    class="Polaris-OptionList-Option__SingleSelectOption"
                  >
                    Downtown
                  </button>
                </li>
              </ul>
            </li>
          </ul>
        </div>
      </div>
    );
  }
}