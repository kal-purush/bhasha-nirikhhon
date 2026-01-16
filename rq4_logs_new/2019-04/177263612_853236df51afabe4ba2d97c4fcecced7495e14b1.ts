import { app, BrowserWindow } from "electron";
import { ApiServer } from "./api-server";
import { parseArgs, ArgsCallback, IConfig } from "./cli-commands";
import { config } from "shelljs";

export const AppCallback: ArgsCallback = (config: IConfig) => {
    const manniApp: ManniApp = new ManniApp(config);
    manniApp.init();
};
export class ManniApp {
    private mainWindow: Electron.BrowserWindow;
    private apiServer: ApiServer;
    public constructor(private readonly config: IConfig) {
        this.apiServer = new ApiServer(config.endpoint.href);
    }

    private createWindow(): void {
        this.apiServer.start();
        // Create the browser window.
        this.mainWindow = new BrowserWindow({
            height: 600,
            icon: __dirname + "/../icon.png",
            minHeight: 480,
            minWidth: 640,
            width: 800,
        });
        // tslint:disable-next-line:no-null-keyword
        this.mainWindow.setMenu(null);
        // and load the index.html of the app.
        // mainWindow.loadFile(path.join(__dirname, "app/index.html"));
        this.mainWindow.loadURL("http://localhost:9482/index.html");

        // Open the DevTools.
        if (this.config.dev === true) {
            this.mainWindow.webContents.openDevTools({
                mode: "right",
            });
        }

        // Emitted when the window is closed.
        this.mainWindow.on("closed", () => {
            // Dereference the window object, usually you would store windows
            // in an array if your app supports multi windows, this is the time
            // when you should delete the corresponding element.
            this.mainWindow = undefined;
        });
    };

    public init(): void {
        app.on("ready", this.createWindow.bind(this));

        app.on("window-all-closed", () => {
            if (process.platform !== "darwin") {
                app.quit();
                this.apiServer.stop();
            }
        });

        app.on("activate", () => {
            if (this.mainWindow === null) {
                this.createWindow();
            }
        });
    }
}