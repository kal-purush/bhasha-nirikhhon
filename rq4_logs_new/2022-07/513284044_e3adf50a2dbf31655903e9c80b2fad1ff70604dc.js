const { app, BrowserWindow, ipcMain } = require("electron");
const path = require("path");
const os = require("os");
const fs = require("fs");

function createWindow() {
  const mainWindow = new BrowserWindow({
    width: 800,
    height: 500,
    show: false,
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
    },
  });

  mainWindow.loadFile("index.html");

  // reduce the garbage collection
  mainWindow.once("ready-to-show", mainWindow.show);

  // other interactions

  // feed dev info
  function changeInDom(user) {
    let code = `
    var p = document.getElementById("developer");
    p.innerHTML = "Developed by: "+"${user}";
    `;
    mainWindow.webContents.executeJavaScript(code);
  }

  changeInDom("[Github] @LexxYungCarter");

  // excluded folders
  function getExcludedFolders() {
    return [
      "dumps",
      "electron",
      "gaming",
      "lodash",
      "meme",
      "offline",
      "resources",
      "standard-wordpress",
      "tailwindui",
    ];
  }

  // subdirectories folders
  function getSubFolders() {
    return [
      "github",
      "ionic",
      "nextjs",
      "nodejs",
      "nodejslive",
      "react-native",
      "typescript",
      "workbench",
    ];
  }

  // global variables
  var folders = [];
  const sitesFolder = path.join(os.homedir(), "Sites");
  const targetDeleteDirectories = ["node_modules", "vendor", "build", "dist"];

  // get folder list
  function getFolderList(folder) {
    const excludedFolders = getExcludedFolders();
    const subFolders = getSubFolders();

    try {
      const getDirectories = (source) =>
        fs
          .readdirSync(source, { withFileTypes: true })
          .filter((dirent) => dirent.isDirectory())
          .map((dirent) => dirent.name);

      const filteredFolders = getDirectories(folder);

      // feed root folders
      folders = filteredFolders
        .filter((dirent) => !excludedFolders.includes(dirent))
        .filter((dirent) => !subFolders.includes(dirent))
        .map((dirent) => {
          return path.join(folder, dirent);
        });

      // include individual subfolders
      subFolders.forEach((subFolder) => {
        const subFolderPath = path.join(folder, subFolder);
        const subFolderList = getDirectories(subFolderPath);
        subFolderList.forEach((subFolder) => {
          folders.push(path.join(subFolderPath, subFolder));
        });
      });

      // append to ul
      function appendToUl(ul, list) {
        list.forEach((item) => {
          const li = document.createElement("li");
          li.innerText = item;
          ul.appendChild(li);
        });
      }

      let code = `
      var p = document.getElementById("folder-list-total");
      p.innerHTML = ""+"${folders.length}";

      var ps = document.getElementById("sub-folder-list-total");
      ps.innerHTML = ""+"${subFolders.length}";
      
      var pe = document.getElementById("excluded-list-total");
      pe.innerHTML = ""+"${excludedFolders.length}";

      var ul = document.getElementById("folder-list");
      ul.innerHTML = ""+"${folders.join("<br/>")}";

      var subUl = document.getElementById("sub-folder-list");
      subUl.innerHTML = ""+"${subFolders.join(", ")}";
      
      var exUl = document.getElementById("excluded-folder-list");
      exUl.innerHTML = ""+"${excludedFolders.join(", ")}";
      
      var tUl = document.getElementById("target-folder-list");
      tUl.innerHTML = ""+"${targetDeleteDirectories.join(", ")}";
      `;
      mainWindow.webContents.executeJavaScript(code);
    } catch (error) {
      console.log("Unable to scan directory: " + error);
    }
  }

  // feed default sites
  function setSitesFolder(folder) {
    let code = `
    var p = document.getElementById("sites-folder");
    p.innerHTML = "${folder}";
    `;
    mainWindow.webContents.executeJavaScript(code);

    getFolderList(folder);
  }

  setSitesFolder(sitesFolder);

  // add event listener
  //ipcMain.on will receive the “btnclick” info from renderprocess
  ipcMain.on("btnclick", function (event, arg) {
    for (let i = 0; i < folders.length; i++) {
      targetDeleteDirectories.forEach((target) => {
        let p = path.join(folders[i], target);

        fs.rm(p, { recursive: true, force: true, maxRetries: 2 }, (err) => {
          if (err) {
            throw err;
          }
          console.log(`${p} is deleted!`);
        });
      });

      event.sender.send("task-status", `${folders[i]} done`);
    }
    console.log("done deleting");
    event.sender.send("btnclick-task-finished");
  });
}

app.whenReady().then(createWindow);

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});