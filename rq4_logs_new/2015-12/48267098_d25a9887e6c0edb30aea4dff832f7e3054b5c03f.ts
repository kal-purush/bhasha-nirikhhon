

export enum EnvironemtType {
    Mac,
    Linux,
    Windows
}

// https://code.visualstudio.com/docs/customization/userandworkspace
export class SettingsFileLocator {
    environment :EnvironemtType;
    
    
    public constructor(enviromentDectection: () => EnvironemtType){
        this.environment = enviromentDectection();
    }
    
    public GetPath() : string {
        
        switch(this.environment){
            case EnvironemtType.Linux:
                return '/.config/Code/User/settings.json';
            case EnvironemtType.Mac:
                 return '/Library/Application Support/Code/User/settings.json';
            case EnvironemtType.Windows:
                return '\Code\User\settings.json'
        }
        
       
    }
    
}