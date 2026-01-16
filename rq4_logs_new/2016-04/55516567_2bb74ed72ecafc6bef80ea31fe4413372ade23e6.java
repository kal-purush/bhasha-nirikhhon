/**
 * Copyright (C) 2016 MIT Libraries
 * Licensed under: http://www.apache.org/licenses/LICENSE-2.0
 */
package edu.mit.lib.felt;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;

import static org.apache.camel.Exchange.*;

import org.fcrepo.client.FcrepoClient;
import org.fcrepo.client.FcrepoResponse;

import static com.google.common.base.Strings.*;

/**
  * Camel CLI app with routes for managing the creation of Fedora4 resources
  * representing electronic (digital) theses. These resources contain metadata,
  * a set of page TIFFs, and a PDF document. The workflow supported here is
  * simple deposit of these artifacts to a local directory observed by this
  * container, which are then converted in a PCDM-modeled collection of objects.
  *
  * @author richardrodgers
  */

public class Felt {

    private final String work;
    private final String repoAddr;
    private final Main main;

    public Felt(Path workDir, String repoAddr) {
        work = workDir.getFileName().toString();
        this.repoAddr = repoAddr;
        main = new Main();
    }

    public void run() {
        main.bind("pcdm", new PCDMObject());
        main.addRouteBuilder(new RouteBuilder() {
            @Override
            public void configure() {
                from("file://" + work + "?include=.*.xml&delay=2000")
                    .setHeader("CamelFcrepoIdentifier", simple("/${file:onlyname.noext}"))
                    .to("direct:createObj")
                    .setHeader("CamelFcrepoIdentifier", simple("/${file:onlyname.noext}/bound"))
                    .to("direct:createObj")
                    .setHeader("CamelFcrepoIdentifier", simple("/${file:onlyname.noext}/bound/files"))
                    .to("direct:createObj")
                    .setHeader("CamelFcrepoIdentifier", simple("/${file:onlyname.noext}/pages"))
                    .to("direct:createObj");

                from("file://" + work + "?include=.*.pdf&delay=2000")
                    .setHeader("CamelFcrepoIdentifier", simple("/${file:onlyname.noext}/bound/files/${file:onlyname}"))
                    .setHeader(HTTP_METHOD, constant("PUT"))
                    .setHeader(CONTENT_TYPE, constant("application/pdf"))
                    .to("fcrepo:" + repoAddr);

                from("file://" + work + "?include=.*.tif&recursive=true&delay=2000")
                    .setHeader("CamelFcrepoIdentifier", simple("/${file:parent}/pages/${file:onlyname.noext}"))
                    .to("direct:createObj")
                    .setHeader("CamelFcrepoIdentifier", simple("/${file:parent}/pages/${file:onlyname.noext}/files"))
                    .to("direct:createObj")
                    .setHeader("CamelFcrepoIdentifier", simple("/${file:parent}/pages/${file:onlyname.noext}/files/${file.onlyname}"))
                    .setHeader(HTTP_METHOD, constant("PUT"))
                    .setHeader(CONTENT_TYPE, constant("image/tiff"))
                    .to("fcrepo:" + repoAddr);

                from("direct:createObj")
                    .bean("pcdm")
                    .setHeader(HTTP_METHOD, constant("PUT"))
                    .setHeader(CONTENT_TYPE, constant("text/turtle"))
                    .to("fcrepo:" + repoAddr);
            }
        });
        try {
            main.run();
        } catch (Exception e) {
            System.out.println("Exception in main#run: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // need a directory to watch and a repo to talk to
        // default to CWD and localhost:8080/rest
        String dir = System.getProperty("user.dir");
        String repo = "localhost:8080/rest";

        for (int i = 0; i < args.length; i++) {
            if ("-w".equals(args[i]) && i < (args.length - 1)) {
                dir = args[i+1];
            } else if ("-r".equals(args[i]) && i < (args.length - 1)) {
                repo = args[i+1];
            } else if (args[i].startsWith("-h")) {
                System.out.println("Use: felt -w <work directory>[cwd] -r <repository address>[localhost:8080/rest]");
                System.exit(1);
            }
        }
        // some sanity checking
        if (isNullOrEmpty(dir) || isNullOrEmpty(repo)) {
            System.out.println("Missing work directory or repository");
            System.exit(1);
        }
        Path workDir = Paths.get(dir);
        if (! workDir.toFile().isDirectory()) {
            System.out.println("File: '" + dir + "' does not exist or is not a directory");
            System.exit(1);
        }
        // can we reach the repo?
        try {
            FcrepoClient client = new FcrepoClient(null, null, "localhost", false);
            FcrepoResponse response = client.get(new URI("http://" + repo), null, null);
            if (response.getStatusCode() != 200) {
                System.out.println("Cannot reach repository at: '" + repo + "'");
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.println("Problem reaching repository at: '" + repo + "'");
            System.exit(1);
        }
        new Felt(workDir, repo).run();
    }
}