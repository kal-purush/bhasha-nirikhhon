import {Component} from "angular2/core";
import {LoansListPage} from "./loans-list-page";

@Component({
    directives: [LoansListPage],
    selector: "my-app",
    template: `
    <TabView>
      <StackLayout *tabItem="{title: 'Loans'}">
        <loans-list-page></loans-list-page>
      </StackLayout>
      <StackLayout *tabItem="{title: 'Fundraising'}">
        <Label text="Content"></Label>
      </StackLayout>
      <StackLayout *tabItem="{title: 'Updates'}">
        <Label text="Content"></Label>
      </StackLayout>
    </TabView>
`,
})
export class AppComponent {
}