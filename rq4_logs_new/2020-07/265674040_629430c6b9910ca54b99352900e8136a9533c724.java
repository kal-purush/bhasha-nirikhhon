package org.monarchinitiative.vmvt.core.svg.ruler;

import org.monarchinitiative.vmvt.core.except.VmvtRuntimeException;

public class AcceptorRuler extends SvgSequenceRuler {

    static final int h = 200;

    public AcceptorRuler(String ref, String alt) {
        super(SVG_DONOR_WIDTH, h, ref, alt);
        if (this.seqlen != ACCEPTOR_NT_LENGTH) {
            throw new VmvtRuntimeException(String.format("Sequence length must be %d for donor but was %d",
                    ACCEPTOR_NT_LENGTH, this.seqlen));
        }
    }

    @Override
    public String getSvg() {
        return null;
    }
}