

import { IState } from 'types/state';


const fs = require('fs');



import { remote } from 'electron';
const Dialog = remote.dialog;

export function saveJSON(state: IState) {

    Dialog.showSaveDialog(remote.getCurrentWindow(), {
        title: '保存',
        // defaultPath: '.',
        filters: [
            { name: 'JSONファイル', extensions: ['json'] }
        ]
    }, (filePath: string) => {

        if (!filePath) return;

        fs.writeFile(filePath, JSON.stringify(state), (err: any) => {
            // 書き出しに失敗した場合
            if (err) {
                console.log("エラーが発生しました。" + err)
                throw err
            }
            // 書き出しに成功した場合
            else {
                console.log("ファイルが正常に書き出しされました")
            }
        });
    });


}







/**
 * プロジェクトを読み込む
 */
export function loadProject() {

    return new Promise((resolve) => {

        Dialog.showOpenDialog(remote.getCurrentWindow(), {
            title: 'プロジェクトを開く',
            // defaultPath: '.',
            filters: [
                { name: 'JSONファイル', extensions: ['json'] }
            ]
        }, (filePaths: string[]) => {

            const filePath = filePaths[0];

            if (!filePath) return;

            fs.readFile(filePath, 'utf8', (err: any, data: string): void => {

                //エラーの場合はエラーを投げてくれる
                if (err) {
                    // throw err;

                    resolve(null);

                    return null;
                }

                resolve(data);

            });

        });
    });


}