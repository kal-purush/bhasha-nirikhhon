import { Template } from '../mixins/template';

const T = Template('d-document', `
<style>
#content {
  counter-reset: table-caption;
}

@media print {
  #content {
    page-break-inside: unset;
  }
}

#content > thead {
  border-bottom: unset;
}

#content tbody :first-child td {
  padding-top: unset;
}

#content > tr > td {
  display: block;
}

.document-header div {
  margin-bottom: 1.0cm;
  border-bottom: 1px solid black;
  font-size: 1rem;
  /*color: #f00;*/
  text-align: left;
}

.document-footer p {
  padding-top: 1.0cm;
  /*border: 1px solid black;*/
}

/*
#header td {
  counter-increment: page-number;
}

#header td::after {
  counter-increment: page-number;
  content: "Page " counter(page-number);
}
*/

.page-num-display {
  counter-increment: page-number;
  text-align: center;
}

.page-num-display::after {
  counter-increment: page-number;
  content: counter(page-number);
}
</style>

<table id="content">
    <thead class="document-header"><tr><td><div><d-header>ICLR 2015</d-header></div>
    <tbody>
        <tr>
            <td><slot></slot></td>
        </tr>
    </tbody>
    <tfoot class="document-footer">
        <tr>
            <td><p class="page-num-display"></p></td>
        </tr>
    </tfoot>
</table>
`);

export class Document extends T(HTMLElement) {

}