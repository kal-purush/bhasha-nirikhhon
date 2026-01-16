package jaeseok.numble.mybox.file.dto;

import jaeseok.numble.mybox.common.util.ByteConvertor;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FileSizeResponse {
    private Double GB;
    private Double MB;
    private Double KB;
    private Long B;

    public FileSizeResponse(Long bytes) {
        this.B = bytes;
        this.KB = ByteConvertor.toKiloBytes(bytes);
        this.MB = ByteConvertor.toMegaBytes(bytes);
        this.GB = ByteConvertor.toGigaBytes(bytes);
    }
}