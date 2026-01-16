/// <reference path="../../../ts/prostyle.d.ts" />

module ProStyle.Extensions.Flows.PageStack {

    import Models = ProStyle.Models;

    export class PageStackFlowModel extends Models.Flows.PlacementFlowModel {

        public static defaultStacksJson = {
            current: {
                position: [0, 0, 0],
                rotation: [0, 0, 0],
                scale: 100,
                opacity: 100
            },
            future: {
                position: [140, 0, -200],
                rotation: [0, -10, 5],
                scale: 100,
                opacity: 80,
                offset: {
                    position: [130, 80, -200],
                    rotation: [0, -5, 2],
                    scale: 100,
                    opacity: 50
                }
            },
            past: {
                position: [-140, 0, -200],
                rotation: [0, 10, -5],
                scale: 100,
                opacity: 80,
                offset: {
                    position: [-130, 80, -200],
                    rotation: [0, 5, -2],
                    scale: 100,
                    opacity: 50
                }
            }
        };

        constructor(story: Models.Story,
                    placement: Types.Placement,
                    defaultPageClass: string,
                    pageAspectRatio: number,
                    public stacks: Types.Stacks) {

            super(story, "pageStack", placement, defaultPageClass, pageAspectRatio, "pageStackPage");
        }
        
        public serialize(): any {
            return PageStack.serialize(this);
        }
        
        public createView(camera: Views.CameraView, flowIndex: number): PageStackFlowView {
            return new PageStackFlowView(this, camera, flowIndex);
        }
    }
}