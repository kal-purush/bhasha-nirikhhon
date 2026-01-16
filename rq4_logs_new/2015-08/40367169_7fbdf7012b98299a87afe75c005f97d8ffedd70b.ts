/// <EXTERNAL REFERENCES>
/// <reference path="node_modules/typefx/build/TypeFramework.d.ts" />

/// <HELPER REFERENCES>
/// <reference path="app/Global.ts" />

/// <MODEL REFERENCES>

/// <CONTROLLER REFERENCES>
/// <reference path="app/controllers/HomeController.ts" />
/// <reference path="app/controllers/ApiController.ts" />

app.addController(HomeController);
app.addController(ApiController);
app.start();