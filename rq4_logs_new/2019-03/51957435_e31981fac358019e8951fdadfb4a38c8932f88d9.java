package org.renci.xostitch;

import org.renci.ahab.libndl.Slice;
import org.renci.ahab.libndl.resources.request.ComputeNode;
import org.renci.ahab.libndl.resources.request.InterfaceNode2Net;
import org.renci.ahab.libndl.resources.request.Network;
import org.renci.ahab.libndl.resources.request.StitchPort;

import org.renci.ahab.libtransport.ISliceTransportAPIv1;
import org.renci.ahab.libtransport.ITransportProxyFactory;
import org.renci.ahab.libtransport.PEMTransportContext;
import org.renci.ahab.libtransport.SSHAccessToken;
import org.renci.ahab.libtransport.SliceAccessContext;
import org.renci.ahab.libtransport.TransportContext;
import org.renci.ahab.libtransport.util.SSHAccessTokenFileFactory;
import org.renci.ahab.libtransport.xmlrpc.XMLRPCProxyFactory;

import java.net.URL;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option.Builder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

/**
 * `mvn clean package`
 * `java -cp ./target/stitch-1.0-SNAPSHOT-jar-with-dependencies.jar org.renci.chameleon.jupyter.stitch.App certLocation keyLocation controllerURL sliceName`
 * Verify slice creation in Flukes
 */
public class XoStitch
{

  enum StitchType
  {
    GENERIC_STITCH, CHAMELEON_STITCH;
  }
  static StitchType type = StitchType.GENERIC_STITCH;

  static String certLocation = null;
  static String keyLocation = null;
  static String controllerUrl = "https://geni.renci.org:11443/orca/xmlrpc";

  static String command = null;
  static String sliceName = null;

  static String sp1_url = null;
  static String sp2_url = null;
  static String sp1_label = null;
  static String sp2_label = null;

  static ISliceTransportAPIv1 sliceProxy;
  static SliceAccessContext<SSHAccessToken> sctx;

    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Creating a simple Slice!" );

        // command line ideas:
        // Chameleon:   ./stitch create -chameleon -tacc 3501 -uc 2391 -cert /path/to/geni/cert
        // Other:       ./stitch create -sp1 <url> -l1 <label> -sp2 <url> -l2 <label> -cert /path/to/cert -controllerURL <url>

        CommandLine commandLine;
        //chameleon options
        Option option_chameleon = Option.builder("chameleon").required(false).argName("chameleon").desc("chameleon").build();
        Option option_chameleon_uc = Option.builder("uc").required(false).argName("uc").hasArg().desc("uc").build();
        Option option_chameleon_tacc = Option.builder("tacc").required(false).argName("tacc").hasArg().desc("tacc").build();

        //non-Chameleon options
        Option option_sp1 = Option.builder("sp1").required(false).argName("stitchport1").hasArg().desc("stitchport1").build();
        Option option_sp2 = Option.builder("sp2").required(false).argName("stitchport2").hasArg().desc("stitchport2").build();
        Option option_sp1_label = Option.builder("l1").required(false).argName("label1").hasArg().desc("label1").build();
        Option option_sp2_label = Option.builder("l2").required(false).argName("label2").hasArg().desc("label2").build();

        //common options
        Option option_certLocation = Option.builder("c").required(false).argName("certLocation").hasArg().desc("certLocation").build();
        Option option_keyLocation = Option.builder("k").required(false).argName("keyLocation").hasArg().desc("keyLocation").build();

        Option option_controllerUrl= Option.builder("u").required(false).argName("controllerUrl").hasArg().desc("controllerUrl").build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(option_chameleon);
        options.addOption(option_chameleon_uc);
        options.addOption(option_chameleon_tacc);

        options.addOption(option_sp1);
        options.addOption(option_sp2);
        options.addOption(option_sp1_label);
        options.addOption(option_sp2_label);
        options.addOption(option_certLocation);
        options.addOption(option_keyLocation);
        options.addOption(option_controllerUrl);

        try
        {
          commandLine = parser.parse(options, args);

          //if chameleon tacc2uc reqeust
          if (commandLine.hasOption("chameleon")){
              System.out.println("Preparing Chameleon TACC-to-UC Circuit");
              type = StitchType.CHAMELEON_STITCH;

              //use exosm
              controllerUrl = "https://geni.renci.org:11443/orca/xmlrpc";

              //uc stitchport
              if (commandLine.hasOption("uc")){
                  System.out.println("UC Label: " + commandLine.getOptionValue("uc"));
                  sp1_url = "http://geni-orca.renci.org/owl/ion.rdf#AL2S/Chameleon/Cisco/6509/GigabitEthernet/1/1";
                  sp1_label = commandLine.getOptionValue("uc");
              }

              //tacc stitchport
              if (commandLine.hasOption("tacc")){
                  System.out.println("TACC Label: " + commandLine.getOptionValue("tacc"));
                  sp2_url = "http://geni-orca.renci.org/owl/ion.rdf#AL2S/TACC/Cisco/6509/TenGigabitEthernet/1/1";
                  sp2_label = commandLine.getOptionValue("tacc");
              }

              //geni cert
              if (commandLine.hasOption("c")){
                  certLocation = commandLine.getOptionValue("c");
                  keyLocation = certLocation;
              }

              sliceName = "chameleon-tacc"+sp2_label+"-uc"+sp1_label;
          } else {
            //Non-Chameleon

          if (commandLine.hasOption("sp1"))
          {
            //System.out.print("Option sp1 is present.  The value is: ");
            //System.out.println(commandLine.getOptionValue("sp1"));
            sp1_url = commandLine.getOptionValue("sp1");
          }
          if (commandLine.hasOption("sp2"))
          {
            sp2_url = commandLine.getOptionValue("sp2");
          }
          if (commandLine.hasOption("l1"))
          {
            sp1_label = commandLine.getOptionValue("l1");
          }
          if (commandLine.hasOption("l2"))
          {
            sp2_label = commandLine.getOptionValue("l2");
          }

        }
          {
            String[] remainder = commandLine.getArgs();
            System.out.print("Remaining arguments: ");
            for (String argument : remainder)
            {
              System.out.print(argument);
              System.out.print(" ");
            }
            if (type == StitchType.GENERIC_STITCH){
              if (remainder.length >= 2){
                command = remainder[0];
                sliceName = remainder[1];
              }
            } else if (type == StitchType.CHAMELEON_STITCH){
              if (remainder.length >= 1){
                command = remainder[0];
              }
            } else {
              System.out.print("Invalid stitch type or missing arguments");
            }

            System.out.println();
          }

        }
        catch (ParseException exception)
        {
          System.out.print("Parse error: ");
          System.out.println(exception.getMessage());
        }

        //buid the exogeni transport context
        buildContext(certLocation, keyLocation);

        //Call command
        if (command.equals("create")){
          XoStitch.createSlice(certLocation,keyLocation,controllerUrl,sliceName);
        } else if (command.equals("delete")){
          XoStitch.deleteSlice(sliceName);
        }else if (command.equals("status")){
          XoStitch.statusSlice(sliceName);
        } else {
          System.out.print("Invalid command: " + command);
        }
      }

    public static void buildContext(String certLocation, String keyLocation) throws Exception{
      /*
       * Get Slice Proxy
       */

      //ExoGENI controller context
      ITransportProxyFactory ifac = new XMLRPCProxyFactory();
      System.out.println("Opening certificate " + certLocation + " and key " + keyLocation);
      char [] pwd = System.console().readPassword("Enter password for key: ");
      TransportContext ctx = new PEMTransportContext(String.valueOf(pwd), certLocation, keyLocation);
      sliceProxy = ifac.getSliceProxy(ctx, new URL(controllerUrl));

      /*
       * SSH Context
       */
      sctx = new SliceAccessContext<>();

      SSHAccessTokenFileFactory fac;
      fac = new SSHAccessTokenFileFactory("~/.ssh/id_rsa.pub", false);

      SSHAccessToken t = fac.getPopulatedToken();
      sctx.addToken("root", "root", t);
      sctx.addToken("root", t);

    }

    public static void deleteSlice(String name) throws Exception{
      System.out.print("deleteSlice: " + name);
      Slice.loadManifestFile(sliceProxy, sliceName).delete();;
    }

    public static void statusSlice(String name){
      System.out.println("statusSlice: " + name);
      try{
        Slice s = Slice.loadManifestFile(sliceProxy, sliceName);

        System.out.println("state: " + s.getState());
      } catch (Exception e){
        System.out.println(e);
      }
      return;
    }

    public static void createSlice(String certLocation, String keyLocation, String controllerUrl, String sliceName ) throws Exception{
      System.out.println("createSlice: " + certLocation + ", " + keyLocation + ", " + controllerUrl + ", " + sliceName);
      System.out.println("sp1: " + sp1_label + ", " + sp1_url);
      System.out.println("sp2: " + sp2_label + ", " + sp2_url);

      Slice slice = Slice.create(sliceProxy,sctx,sliceName);

      StitchPort sp1 = slice.addStitchPort("sp1",sp1_label,sp1_url,10000000);
      StitchPort sp2 = slice.addStitchPort("sp2",sp2_label,sp2_url,10000000);

      sp1.stitch(sp2);
      //System.out.println(slice.getRequest());
      slice.commit();
    }
}