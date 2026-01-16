export const seasonStyle = (season: number, field: string = 'backgroundColor'): any => {
    switch(season){
        case 1:
            return {[field]: '#F3B136'}
        case 2:
            return {[field]: '#FF5F97'}
        default: return {[field]: '#F3B136'}
    }

}