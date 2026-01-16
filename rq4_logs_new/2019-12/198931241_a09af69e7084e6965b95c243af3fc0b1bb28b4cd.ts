module.exports = (fs: any, readline: any, userConfig: any) => {

    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    const openFilesOnImport = () => {
        return new Promise((resolve, reject) => {
            rl.question("would you like to open snippets when they are imported? (Y/N): ", (answer: string) => {
                updateUserConfig(answer, 'openFilesOnImport');
                resolve()
            })
        })
    }

    const copyToClipboard = () => {
        return new Promise((resolve, reject) => {
            rl.question("would you like to have snippet contents save to your clipboard? (Y/N): ", (answer: string) => {
                updateUserConfig(answer, 'copyContentsToClipBoard');
                resolve()
            })
        })
    }

    const assignDefaultEditorPath = () => {
        return new Promise((resolve, reject) => {
            rl.question("define the path to the code editor we should open imported/existing snippets in: ", (answer: string) => {
                userConfig.defaultEditor = answer;
                resolve()
            })
        })
    }

    function updateUserConfig(answer: string, userPropertyToUpdate: any) {
        if (answer.toLocaleLowerCase() == 'y' || answer.toLocaleLowerCase() == 'yes') {
            userConfig[userPropertyToUpdate] = true;
        }
        if (answer.toLocaleLowerCase() == 'n' || answer.toLocaleLowerCase() == 'no') {
            userConfig[userPropertyToUpdate] = false;
        }
    }

    return {
        configureUser: async (userConfigFilePath: string) => {
            await openFilesOnImport();
            await copyToClipboard();
            await assignDefaultEditorPath();
            rl.close();
            userConfig.userIsConfigured = true;
            fs.writeFileSync(userConfigFilePath, JSON.stringify(userConfig, null, 2));
        }
    }
}
