export interface ITemplateStorage
{


	isTemplateExists(name: string): boolean;

	save(name: string, templateType: any): void;

	getTemplate(name: string): any;

}