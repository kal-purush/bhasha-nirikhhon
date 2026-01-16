import { Component, OnInit } from '@angular/core';
import { ComponentBase } from '@base/component.base';
import { TestService } from '@services/new-test.service';
import { TestPreviewResultModel } from 'src/result-models/test-preview.result-model';

@Component({
	selector: 'q-my-tests',
	templateUrl: './my-tests.component.html',
	styleUrls: ['./my-tests.component.scss']
})
export class MyTestsComponent extends ComponentBase implements OnInit
{
	private readonly _testService: TestService;

	private _testPreviews: TestPreviewResultModel[];
	private _isLoaded: boolean = false;

	public constructor(testService: TestService)
	{
		super();
		this._testService = testService;
	}

	public get loaded(): boolean
	{
		return this._isLoaded;
	}

	public get testPreviews(): TestPreviewResultModel[]
	{
		return this._testPreviews;
	}

	public ngOnInit(): void
	{
		this._testService.getOwnTests().subscribe(r =>
		{
			this._testPreviews = r;
			this._isLoaded = true;
		});
	}
}