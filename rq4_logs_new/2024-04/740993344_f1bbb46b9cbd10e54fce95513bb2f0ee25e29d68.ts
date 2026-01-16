import { appColors } from "../constants/appColors";

export function formatTrangThaiColor(trangThai: number): string {
    switch (trangThai) {
        case 0:
            return appColors.red;
        case 1:
            return appColors.primary;
        default:
            return appColors.text;
    }
}

export function formatTrangThaiGiaoHangColor(trangThai: number): string {
    switch (trangThai) {
        case 0:
            return appColors.red;
        case 1:
            return appColors.text;
        case 2:
            return appColors.text;
        case 3:
            return appColors.primary;
        case 4:
            return appColors.red;
        default:
            return "---";
    }
}

export function formatTrangThaiThanhToanColor(trangThai: number): string {
    switch (trangThai) {
        case 0:
            return appColors.red;
        case 1:
            return appColors.primary;
        default:
            return "---";
    }
}