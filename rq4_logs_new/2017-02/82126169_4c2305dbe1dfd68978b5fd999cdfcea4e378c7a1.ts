import {Component} from '@angular/core';

@Component({
    selector: 'niwa-interface-footer',
    providers: [],
    template: `
<footer role="contentinfo">
	<div class="container-fluid">
		<div class="row">
			<div class="col-md-8 col-xs-8">
				<ul>
					<li>VERSION NUMBER</li>
					<li id="copyrightNiwa">Copyright 2017 NIWA</li>
				</ul>
			</div>
			<div class="col-md-4 col-xs-4">
				<!--<ul class="pull-right">
                    <li >right footer notes</li>
                </ul>-->
			</div>
		</div>
	</div>
	<style type="text/css">
		body { margin-bottom: 60px; }
	</style>
</footer>
`
})
export class NiwaInterfaceFooterComponent {


    constructor() {
    }

    ngOnInit() {


    }




}