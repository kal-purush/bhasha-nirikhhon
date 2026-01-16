package org.monarchinitiative.vmvt.core.svg.walker;

import org.monarchinitiative.vmvt.core.except.VmvtRuntimeException;
import org.monarchinitiative.vmvt.core.pssm.DoubleMatrix;
import org.monarchinitiative.vmvt.core.svg.AbstractSvgGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Generate an SVG graphic that shows the canonical splice site as well as the cryptic splice site together with
 * their individual information content R_i
 * @author Peter N Robinson
 */
public class SvgCanonicalCrypticGenerator extends AbstractSvgGenerator {

    private static final int  SVG_CANONICAL_CRYPTIC_HEIGHT = 340;

    private final String canonical;
    private final String cryptic;
    private final DoubleMatrix splicesite;
    private final boolean isDonor;

    private SvgCanonicalCrypticGenerator(int w, int h, String can, String crypt, DoubleMatrix site) {
        super(w, h);
        this.canonical = can;
        this.cryptic = crypt;
        this.splicesite = site;
        if (site.getMotifLength() == DONOR_NT_LENGTH) {
            isDonor = true;
        } else if (site.getMotifLength() == ACCEPTOR_NT_LENGTH) {
            isDonor = false;
        } else {
            throw new VmvtRuntimeException("Invalid splice site length (not 9 or 27):" + site.getMotifLength());
        }
    }


    private void writeDonor(Writer writer) throws IOException {
        // Note the API of the WalkerGenerators requires ref and alt but for
        // now we just want to show one sequence that we treat as ref
        // todo -- refactor
        int ystart = AbstractSvgGenerator.SVG_WALKER_STARTY;
        SvgSequenceWalker walker = SvgSequenceWalker.singleDonorWalker(this.canonical, this.splicesite, ystart);
        walker.writeRefWalker(writer);
        ystart += SVG_WALKER_HEIGHT;
        walker = SvgSequenceWalker.singleDonorWalker(this.cryptic, this.splicesite, ystart);
        walker.writeRefWalker(writer);
    }


    private void writeAcceptor(Writer writer) throws IOException {
        // Note the API of the WalkerGenerators requires ref and alt but for
        // now we just want to show one sequence that we treat as ref
        // todo -- refactor
        int ystart = AbstractSvgGenerator.SVG_WALKER_STARTY;
        SvgSequenceWalker walker = SvgSequenceWalker.singleAcceptorWalker(this.canonical, this.splicesite, ystart);
        walker.writeRefWalker(writer);
        ystart += SVG_WALKER_HEIGHT;
        walker = SvgSequenceWalker.singleAcceptorWalker(this.cryptic, this.splicesite, ystart);
        walker.writeRefWalker(writer);
    }


    @Override
    public String getSvg() {
        StringWriter swriter = new StringWriter();
        try {
            writeHeader(swriter);
            write(swriter);
            writeFooter(swriter);
            return swriter.toString();
        } catch (IOException e) {
            return getSvgErrorMessage(e.getMessage());
        }
    }

    @Override
    public void write(Writer writer) throws IOException {
        if (isDonor) {
            writeDonor(writer);
        } else {
            writeAcceptor(writer);
        }
    }


    public static SvgCanonicalCrypticGenerator donor(String can, String crypt, DoubleMatrix donor) {
        SvgCanonicalCrypticGenerator generator =
                new SvgCanonicalCrypticGenerator(SVG_DONOR_WIDTH, SVG_CANONICAL_CRYPTIC_HEIGHT, can, crypt,donor);
        return generator;
    }

    public static SvgCanonicalCrypticGenerator acceptor(String can, String crypt, DoubleMatrix acceptor) {
        return new SvgCanonicalCrypticGenerator(SVG_ACCEPTOR_WIDTH, SVG_CANONICAL_CRYPTIC_HEIGHT, can, crypt, acceptor);
    }




}