import { Injectable } from "@angular/core";
import { MatDialogRef } from "@angular/material/dialog";
import { PepColorService } from "@pepperi-addons/ngx-lib";
import { PepDialogData, PepDialogService } from "@pepperi-addons/ngx-lib/dialog";
import { Color } from "../app/models/color";


@Injectable({
    providedIn: 'root',
})
export class DataVisualizationService {
    dialogRef: MatDialogRef<any>;
    constructor(private pepColorService: PepColorService,
        private dialogService: PepDialogService) { };

    getRGBAcolor(colObj: Color, opac = null) {
        let rgba = 'rgba(255,255,255,0';
        if (colObj) {
            let color = colObj.color;
            let opacity = opac != null ? opac : parseInt(colObj.opacity);

            opacity = opacity > 0 ? opacity / 100 : 0;
            //check if allready rgba

            let hsl = this.pepColorService.hslString2hsl(color);
            let rgb = this.pepColorService.hsl2rgb(hsl);

            rgba = 'rgba(' + rgb.r + ',' + rgb.g + ',' + rgb.b + ',' + opacity + ')';
        }
        return rgba;
    }

    changeCursorOnDragStart() {
        document.body.classList.add('inheritCursors');
        document.body.style.cursor = 'grabbing';
    }

    changeCursorOnDragEnd() {
        document.body.classList.remove('inheritCursors');
        document.body.style.cursor = 'unset';
    }

    getCardShadow(intensity, type) {
        let shadow = type === 'Soft' ? '0px 3px 6px 0px rgba(0, 0, 0, ' + intensity + '),0px 4px 8px 0px rgba(0, 0, 0, ' + intensity + '),0px 6px 12px 0px rgba(0, 0, 0, ' + intensity + ')' :
            '0px 8px 16px 0px rgba(0, 0, 0, ' + intensity + '), 0px 12px 24px 0px rgba(0, 0, 0, ' + intensity + '),0px 24px 48px 0px rgba(0, 0, 0, ' + intensity + ')'

        return `${shadow}`;
    }

    deepCloneObject(object) {
        var str = JSON.stringify(object)
        return JSON.parse(str) ;
    }
    
    openDialog(title, content, buttons, input, callbackFunc = null): void {
        const config = this.dialogService.getDialogConfig(
            {
                disableClose: true,
                panelClass: 'pepperi-standalone'
            },
            'inline'
        );
        const data = new PepDialogData({
            title: title,
            content: content,
            actionButtons: buttons,
            actionsType: "custom",
            showHeader: true,
            showFooter: true,
            showClose: true
        })
        config.data = data;

        this.dialogRef = this.dialogService.openDialog(content, input, config);
        this.dialogRef.afterClosed().subscribe(res => {
            callbackFunc(res);
        });
    }


    getChartBorder(useBorder, border) {
        if (useBorder) {
            let col: Color = border;
            return '1px solid ' + this.getRGBAcolor(col);
        }
        else {
            return 'none';
        }
    }

}