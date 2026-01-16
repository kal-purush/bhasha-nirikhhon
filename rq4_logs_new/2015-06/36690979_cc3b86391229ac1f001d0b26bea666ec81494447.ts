
module babylonTest {

    export function init() {
        var canvas: any = document.getElementById("renderCanvas");
        var engine = new BABYLON.Engine(canvas, true);
        //var scene = new BABYLON.Scene(engine);
        //new BABYLON.FreeCamera("mainCamera", new BABYLON.Vector3(0, 0, 0), scene);
        // ("Camera", 0, 0, 10, new BABYLON.Vector3(0, 0, 0), scene);
        //var camera = new BABYLON.Camera("mainCamera", new BABYLON.Vector3(0, 0, 0), scene);

        /*var camera = new BABYLON.ArcRotateCamera("Camera", 0, 0, 10, new BABYLON.Vector3(0, 0, 0), scene);
      
        camera.attachControl(canvas);*/

        BABYLON.SceneLoader.Load("/areas/muvegl/worlds/ExternalAssets/Graphics/Scenes/BriefingRoom/Base/", "BriefingRoom.babylon", engine, function (scene) {
            FCC.MainMenu.load();
            var camera = new BABYLON.ArcRotateCamera("Camera", 0, 0, 10, new BABYLON.Vector3(0, 0, 0), scene);

            camera.attachControl(canvas);

            var renderLoop = function () {
                scene.render();
            };
            scene.activeCamera = camera;
    
            // Launch render loop
            engine.runRenderLoop(renderLoop);
            $(function () {

                FCC.Map.init([//the name does not include the namespace
                    { name: 'BriefingRoom', title: 'Briefing Room', srcIcon: 'BriefingRoomLabelBlue', active: true, enabled: true },
                    { name: 'CoffeeShop', title: 'CoffeeShop', srcIcon: 'CoffeeShopLabelBlue', enabled: true },
                    { name: 'CommandPost', title: 'Command Post', srcIcon: 'CommandPostLabelBlue' },
                    { name: 'HomeOffice', title: 'Office', srcIcon: 'HomeOfficeLabelBlue', enabled: true },
                    { name: 'PRA', title: 'PRA', srcIcon: 'PRALabelBlue' },
                    { name: 'VAMCOffice', title: 'Vamc Office', srcIcon: 'VAMCDirectorLabelBlue' },
                ]);

            });

            MUVEGL.voip.startConnection(null, null,(): void => {
                MuveBabylon.EventSystem.raise(MuveBabylon.Constants.events.MUVEGL.namespace, MuveBabylon.Constants.events.MUVEGL.voipReady);
            });

            MUVEGL.Chat.launch();

        }, null, function (scene) { console.log("Error loading"); });

        //var light0 = new BABYLON.PointLight("Omni0", new BABYLON.Vector3(0, 100, 100), scene);
      
        //scene.activeCameras.push(<any>camera);
    
        // Render loop
        

    }
}