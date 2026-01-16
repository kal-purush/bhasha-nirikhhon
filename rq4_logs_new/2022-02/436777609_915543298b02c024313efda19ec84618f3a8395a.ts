import { RenderView } from '../Renderer/RenderView';
import { EditorEntity } from '../world/EditorEntity';
import { IPage } from './interfaces/IPage';
import { IPageData } from './interfaces/IPageData';

/**
 * Base compoenent Class resposible with rendering/updating
 */
export abstract class BasePage implements IPage {
    protected _page: EditorEntity;
    protected _data: IPageData;

    public name: string;

    /**
     * class constructor
     * @param {IComponentData} data
     */
    public constructor(data: IPageData) {
        this._data = data;
        this.name = data.name;
    }

    /**
     * get owner
     */
    public get page(): EditorEntity {
        return this._page;
    }

    /**
     * set owner
     * @param {EditorEntity} page
     */
    setOwner(page: EditorEntity): void {
        this._page = page;
    }

    /**
     * load
     */
    public load(): void {}

    /**
     * update method
     * @param {number} time
     */
    public update(time: number): void {}

    /**
     * render method
     * @param {RenderView} renderView
     */
    public render(renderView: RenderView): void {}
}