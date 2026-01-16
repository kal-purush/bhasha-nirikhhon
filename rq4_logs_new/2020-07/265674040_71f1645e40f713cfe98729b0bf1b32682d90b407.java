package org.monarchinitiative.vmvt.cli.commands;

import org.monarchinitiative.vmvt.core.VmvtGenerator;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Print an SVG with the donor or acceptor logo.
 * Note that this only shows the wildtype sequence and so the user
 * does not need to pass sequences
 */
@CommandLine.Command(name = "logo", aliases = {"L"}, description = "Create sequence logo")
public class LogoCommand implements Callable<Integer> {
    @CommandLine.Option(names = "--donor") boolean isDonor = true;
    @CommandLine.Option(names = "--acceptor") boolean isAcceptor = false;
    @CommandLine.Option(names = {"-o","--out"}) String outname = "logo.svg";

    @Override
    public Integer call() throws Exception {
        System.out.println(isDonor);
        VmvtGenerator vmvt = new VmvtGenerator();
        String svg;
        if (isAcceptor) {
            isDonor = false;
        }
        if (isDonor) {
            svg = vmvt.getDonorLogoSvg("a","c");
        } else {
            svg = vmvt.getAcceptorLogoSvg("a","c");
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outname))) {
            writer.write(svg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}