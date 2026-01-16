using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GenSyncedNukeFile
{
    public class FinalCutProXMLBuilder
    {
        void Usage()
        {
            Console.WriteLine("Usage: GenPluralEyesImportXML directory_to_cam_mp4s");
        }


        ////////////////////////////////////////////////
        /////
        ///

        string gXMLHeader =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<!DOCTYPE xmeml>\n" +
    "<xmeml version=\"4\">\n" +
    "	<project>\n" +
    "		<name>GroundUpFromPremiere</name>\n" +
    "		<children>\n" +
    "			<sequence id=\"sequence-1\" TL.SQAudioVisibleBase=\"0\" TL.SQVideoVisibleBase=\"0\" TL.SQVisibleBaseTime=\"0\" TL.SQAVDividerPosition=\"0.5\" TL.SQHideShyTracks=\"0\" TL.SQHeaderWidth=\"164\" Monitor.ProgramZoomOut=\"152409600000000\" Monitor.ProgramZoomIn=\"0\" TL.SQTimePerPixel=\"0.2\" MZ.EditLine=\"0\" MZ.Sequence.PreviewFrameSizeHeight=\"1080\" MZ.Sequence.PreviewFrameSizeWidth=\"1920\" MZ.Sequence.AudioTimeDisplayFormat=\"200\" MZ.Sequence.VideoTimeDisplayFormat=\"106\" MZ.Sequence.PreviewRenderingClassID=\"1297106761\" MZ.Sequence.PreviewRenderingPresetCodec=\"1297107278\" MZ.Sequence.PreviewRenderingPresetPath=\"EncoderPresets\\SequencePreview\\088bb726-9823-467e-b3b7-26f3703d4cf1\\I-Frame Only MPEG.epr\" MZ.Sequence.PreviewUseMaxRenderQuality=\"false\" MZ.Sequence.PreviewUseMaxBitDepth=\"false\" MZ.Sequence.EditingModeGUID=\"088bb726-9823-467e-b3b7-26f3703d4cf1\" MZ.WorkOutPoint=\"5593940352000\" MZ.WorkInPoint=\"0\">\n" +
    "				<uuid>cd055a26-5a9c-4e60-9d7e-f84f7ac32872</uuid>\n";

        string gXMLHeader2 =
    "				<duration>{0}</duration>\n";

        string gXMLHeader3 =
    "				<rate>\n" +
    "					<timebase>60</timebase>\n" +
    "					<ntsc>TRUE</ntsc>\n" +
    "				</rate>\n" +
    "				<name>Sequence 01</name>\n" +
    "				<media>\n" +
    "					<video>\n" +
    "						<format>\n" +
    "							<samplecharacteristics>\n" +
    "								<rate>\n" +
    "									<timebase>60</timebase>\n" +
    "									<ntsc>TRUE</ntsc>\n" +
    "								</rate>\n" +
    "								<codec>\n" +
    "									<name>Apple ProRes 422</name>\n" +
    "									<appspecificdata>\n" +
    "										<appname>Final Cut Pro</appname>\n" +
    "										<appmanufacturer>Apple Inc.</appmanufacturer>\n" +
    "										<appversion>7.0</appversion>\n" +
    "										<data>\n" +
    "											<qtcodec>\n" +
    "												<codecname>Apple ProRes 422</codecname>\n" +
    "												<codectypename>Apple ProRes 422</codectypename>\n" +
    "												<codectypecode>apcn</codectypecode>\n" +
    "												<codecvendorcode>appl</codecvendorcode>\n" +
    "												<spatialquality>1024</spatialquality>\n" +
    "												<temporalquality>0</temporalquality>\n" +
    "												<keyframerate>0</keyframerate>\n" +
    "												<datarate>0</datarate>\n" +
    "											</qtcodec>\n" +
    "										</data>\n" +
    "									</appspecificdata>\n" +
    "								</codec>\n" +
    "								<width>1920</width>\n" +
    "								<height>1080</height>\n" +
    "								<anamorphic>FALSE</anamorphic>\n" +
    "								<pixelaspectratio>square</pixelaspectratio>\n" +
    "								<fielddominance>none</fielddominance>\n" +
    "								<colordepth>24</colordepth>\n" +
    "							</samplecharacteristics>\n" +
    "						</format>\n";

        // 0 - "							<clipitem id=\"clipitem-2\" frameBlend=\"FALSE\">\n" +
        // 1 - "								<masterclipid>masterclip-2</masterclipid>\n" +
        // 2 - "								<name>Shot01_cam_02.MP4</name>\n" +
        // 3 - "								<duration>1320</duration>\n" +
        // 4 - "								<end>1320</end>\n" +
        // 5 - "								<out>1320</out>\n" +
        // 6 - "								<file id=\"file-2\">\n" +
        // 7 - "									<name>Shot01_cam_02.MP4</name>\n" +
        // 8 - "									<pathurl>file://localhost/Z%3a/CreativeStudioJobs/VR_Flex360/08_TRANSFER/EXTERNAL/SHOOT/Flexi_20160728/SortedCameras/Shot_01/Shot01_cam_02.MP4</pathurl>\n" +
        // 9 - "									<duration>1320</duration>\n" +
        // 10 - "									<linkclipref>clipitem-2</linkclipref>\n" +
        // 11 - "									<trackindex>2</trackindex>\n" +
        // 12 - "									<linkclipref>clipitem-8</linkclipref>\n" +
        // 13 - "									<trackindex>3</trackindex>\n" +
        // 14 - "									<linkclipref>clipitem-9</linkclipref>\n" +
        // 15 - "									<trackindex>4</trackindex>\n" +
        string gXMLVideoFileTrack =
    "						<track TL.SQTrackShy=\"0\" TL.SQTrackCollapsedHeight=\"25\" TL.SQTrackExpanded=\"0\" TL.SQTrackVideoKeyframeStyle=\"0\" TL.SQTrackVideoDisplayStyle=\"1\" MZ.TrackTargeted=\"0\">\n" +
    "							<enabled>TRUE</enabled>\n" +
    "							<locked>FALSE</locked>\n" +
    "							<clipitem id=\"{0}\" frameBlend=\"FALSE\">\n" +
    "								<masterclipid>{1}</masterclipid>\n" +
    "								<name>{2}</name>\n" +
    "								<enabled>TRUE</enabled>\n" +
    "								<duration>{3}</duration>\n" +
    "								<start>0</start>\n" +
    "								<end>{4}</end>\n" +
    "								<in>0</in>\n" +
    "								<out>{5}</out>\n" +
    "								<alphatype>black</alphatype>\n" +
    "								<pixelaspectratio>square</pixelaspectratio>\n" +
    "								<anamorphic>FALSE</anamorphic>\n" +
    "								<file id=\"{6}\">\n" +
    "									<name>{7}</name>\n" +
    "									<pathurl>{8}</pathurl>\n" +
    "									<rate>\n" +
    "										<timebase>60</timebase>\n" +
    "										<ntsc>TRUE</ntsc>\n" +
    "									</rate>\n" +
    "									<duration>{9}</duration>\n" +
    "									<timecode>\n" +
    "										<rate>\n" +
    "											<timebase>60</timebase>\n" +
    "											<ntsc>TRUE</ntsc>\n" +
    "										</rate>\n" +
    "										<string>00:00:00:00</string>\n" +
    "										<frame>0</frame>\n" +
    "										<displayformat>NDF</displayformat>\n" +
    "										<reel>\n" +
    "											<name></name>\n" +
    "										</reel>\n" +
    "									</timecode>\n" +
    "									<media>\n" +
    "										<video>\n" +
    "											<duration>18000</duration>\n" +
    "											<samplecharacteristics>\n" +
    "												<rate>\n" +
    "													<timebase>60</timebase>\n" +
    "													<ntsc>TRUE</ntsc>\n" +
    "												</rate>\n" +
    "												<width>1920</width>\n" +
    "												<height>1080</height>\n" +
    "												<anamorphic>FALSE</anamorphic>\n" +
    "												<pixelaspectratio>square</pixelaspectratio>\n" +
    "												<fielddominance>none</fielddominance>\n" +
    "											</samplecharacteristics>\n" +
    "										</video>\n" +
    "										<audio>\n" +
    "											<samplecharacteristics>\n" +
    "												<depth>16</depth>\n" +
    "												<samplerate>48000</samplerate>\n" +
    "											</samplecharacteristics>\n" +
    "											<channelcount>2</channelcount>\n" +
    "										</audio>\n" +
    "									</media>\n" +
    "								</file>\n" +
    "								<link>\n" +
    "									<linkclipref>{10}</linkclipref>\n" +
    "									<mediatype>video</mediatype>\n" +
    "									<trackindex>{11}</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{12}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>{13}</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{14}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>{15}</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "							</clipitem>\n" +
    "						</track>\n";

        string gXMLVideoToAudio =
    "					</video>\n" +
    "					<audio>\n" +
    "						<format>\n" +
    "							<samplecharacteristics>\n" +
    "								<depth>16</depth>\n" +
    "								<samplerate>48000</samplerate>\n" +
    "							</samplecharacteristics>\n" +
    "						</format>\n" +
    "						<outputs>\n" +
    "							<group>\n" +
    "								<index>1</index>\n" +
    "								<numchannels>1</numchannels>\n" +
    "								<downmix>0</downmix>\n" +
    "								<channel>\n" +
    "									<index>1</index>\n" +
    "								</channel>\n" +
    "							</group>\n" +
    "							<group>\n" +
    "								<index>2</index>\n" +
    "								<numchannels>1</numchannels>\n" +
    "								<downmix>0</downmix>\n" +
    "								<channel>\n" +
    "									<index>2</index>\n" +
    "								</channel>\n" +
    "							</group>\n" +
    "						</outputs>\n";




        // 0 - "							<clipitem id=\"clipitem-8\" frameBlend=\"FALSE\" PannerCurrentValue=\"0.5\" PannerKeyframes=\"\" PannerStartKeyframe=\"-91445760000000000,0.5,0,0,0,0,0,0\" PannerIsInverted=\"true\" PannerName=\"Balance\">\n" +
        // 1 - "								<masterclipid>masterclip-2</masterclipid>\n" +
        // 2 - "								<name>Shot01_cam_02.MP4</name>\n" +
        // 3 - "								<duration>1320</duration>\n" +
        // 4 - "								<end>1320</end>\n" +
        // 5 - "								<out>1320</out>\n" +
        // 6 - "								<file id=\"file-2\"/>\n" +
        // 7 - "									<linkclipref>clipitem-2</linkclipref>\n" +
        // 8 - "									<trackindex>{8}</trackindex>\n" +
        // 9 - "									<linkclipref>clipitem-8</linkclipref>\n" +
        // 10 - "									<trackindex>3</trackindex>\n" +
        // 11 - "									<linkclipref>clipitem-9</linkclipref>\n" +
        // 12 - "									<trackindex>4</trackindex>\n" +
        // 13 - 1 for first, 2 for 2nd
        string gXMLAudioTrack =
    "						<track TL.SQTrackAudioKeyframeStyle=\"0\" TL.SQTrackAudioDisplayStyle=\"0\" TL.SQTrackShy=\"0\" TL.SQTrackCollapsedHeight=\"25\" TL.SQTrackExpanded=\"0\" MZ.TrackTargeted=\"0\" PannerCurrentValue=\"0.5\" PannerIsInverted=\"true\" PannerKeyframes=\"\" PannerStartKeyframe=\"-91445760000000000,0.5,0,0,0,0,0,0\" PannerName=\"Balance\" currentExplodedTrackIndex=\"{14}\" totalExplodedTrackCount=\"2\" premiereTrackType=\"Stereo\">\n" +
    "							<enabled>TRUE</enabled>\n" +
    "							<locked>FALSE</locked>\n" +
    "							<clipitem id=\"{0}\" frameBlend=\"FALSE\" PannerCurrentValue=\"0.5\" PannerKeyframes=\"\" PannerStartKeyframe=\"-91445760000000000,0.5,0,0,0,0,0,0\" PannerIsInverted=\"true\" PannerName=\"Balance\">\n" +
    "								<masterclipid>{1}</masterclipid>\n" +
    "								<name>{2}</name>\n" +
    "								<enabled>TRUE</enabled>\n" +
    "								<duration>{3}</duration>\n" +
    "								<start>0</start>\n" +
    "								<end>{4}</end>\n" +
    "								<in>0</in>\n" +
    "								<out>{5}</out>\n" +
    "								<file id=\"{6}\"/>\n" +
    "								<sourcetrack>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>{13}</trackindex>\n" +
    "								</sourcetrack>\n" +
    "									<filter>\n" +
    "										<effect>\n" +
    "											<name>Audio Pan</name>\n" +
    "											<effectid>audiopan</effectid>\n" +
    "											<effectcategory>audiopan</effectcategory>\n" +
    "											<effecttype>audiopan</effecttype>\n" +
    "											<mediatype>audio</mediatype>\n" +
    "											<parameter authoringApp=\"PremierePro\">\n" +
    "												<parameterid>pan</parameterid>\n" +
    "												<name>Pan</name>\n" +
    "												<valuemin>-1</valuemin>\n" +
    "												<valuemax>1</valuemax>\n" +
    "												<value>0</value>\n" +
    "											</parameter>\n" +
    "										</effect>\n" +
    "									</filter>\n" +
    "								<link>\n" +
    "									<linkclipref>{7}</linkclipref>\n" +
    "									<mediatype>video</mediatype>\n" +
    "									<trackindex>{8}</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{9}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>{10}</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{11}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>{12}</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "							</clipitem>\n" +
    "							<outputchannelindex>{13}</outputchannelindex>\n" +
    "						</track>\n";

        string gXMLAudioToList =
    "					</audio>\n" +
    "				</media>\n" +
    "				<timecode>\n" +
    "					<rate>\n" +
    "						<timebase>60</timebase>\n" +
    "						<ntsc>TRUE</ntsc>\n" +
    "					</rate>\n" +
    "					<string>00:00:00:00</string>\n" +
    "					<frame>0</frame>\n" +
    "					<displayformat>NDF</displayformat>\n" +
    "				</timecode>\n" +
    "			</sequence>\n";

        // 0 - "			<clip id=\"masterclip-2\" frameBlend=\"FALSE\">\n" +
        // 1 - "				<masterclipid>masterclip-2</masterclipid>\n" +
        // 2 - "				<duration>1320</duration>\n" +
        // 3 - "				<name>Shot01_cam_02.MP4</name>\n" +
        // 4 - "							<clipitem id=\"clipitem-19\" frameBlend=\"FALSE\">\n" +
        // 5 - "								<masterclipid>masterclip-2</masterclipid>\n" +
        // 6 - "								<name>Shot01_cam_02.MP4</name>\n" +
        // 7 - "								<file id=\"file-2\"/>\n" +
        // 8 - "									<linkclipref>clipitem-19</linkclipref>\n" +
        // 9 - "									<linkclipref>clipitem-20</linkclipref>\n" +
        // 10 - "									<linkclipref>clipitem-21</linkclipref>\n" +
        // 11 - "							<clipitem id=\"clipitem-20\" frameBlend=\"FALSE\">\n" +
        // 12 - "								<masterclipid>masterclip-2</masterclipid>\n" +
        // 13 - "								<name>Shot01_cam_02.MP4</name>\n" +
        // 14 - "								<file id=\"file-2\"/>\n" +
        // 15 - "									<linkclipref>clipitem-19</linkclipref>\n" +
        // 16 - "									<linkclipref>clipitem-20</linkclipref>\n" +
        // 17 - "									<linkclipref>clipitem-21</linkclipref>\n" +
        // 18 - "							<clipitem id=\"clipitem-21\" frameBlend=\"FALSE\">\n" +
        // 19 - "								<masterclipid>masterclip-2</masterclipid>\n" +
        // 20 - "								<name>Shot01_cam_02.MP4</name>\n" +
        // 21 - "								<file id=\"file-2\"/>\n" +
        // 22 - "									<linkclipref>clipitem-19</linkclipref>\n" +
        // 23 - "									<linkclipref>clipitem-20</linkclipref>\n" +
        // 24 - "									<linkclipref>clipitem-21</linkclipref>\n" +
        string gXMLClipList =
    "			<clip id=\"{0}\" frameBlend=\"FALSE\">\n" +
    "				<uuid>15cf88c0-4799-40dc-8c91-9cff2523d10a</uuid>\n" +
    "				<masterclipid>{1}</masterclipid>\n" +
    "				<ismasterclip>TRUE</ismasterclip>\n" +
    "				<duration>{2}</duration>\n" +
    "				<rate>\n" +
    "					<timebase>60</timebase>\n" +
    "					<ntsc>TRUE</ntsc>\n" +
    "				</rate>\n" +
    "				<name>{3}</name>\n" +
    "				<media>\n" +
    "					<video>\n" +
    "						<track>\n" +
    "							<clipitem id=\"{4}\" frameBlend=\"FALSE\">\n" +
    "								<masterclipid>{5}</masterclipid>\n" +
    "								<name>{6}</name>\n" +
    "								<alphatype>black</alphatype>\n" +
    "								<file id=\"{7}\"/>\n" +
    "								<link>\n" +
    "									<linkclipref>{8}</linkclipref>\n" +
    "									<mediatype>video</mediatype>\n" +
    "									<trackindex>1</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{9}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>1</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{10}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>2</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "							</clipitem>\n" +
    "						</track>\n" +
    "					</video>\n" +
    "					<audio>\n" +
    "						<track>\n" +
    "							<clipitem id=\"{11}\" frameBlend=\"FALSE\">\n" +
    "								<masterclipid>{12}</masterclipid>\n" +
    "								<name>{13}</name>\n" +
    "								<file id=\"{14}\"/>\n" +
    "								<sourcetrack>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>1</trackindex>\n" +
    "								</sourcetrack>\n" +
    "								<link>\n" +
    "									<linkclipref>{15}</linkclipref>\n" +
    "									<mediatype>video</mediatype>\n" +
    "									<trackindex>1</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{16}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>1</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{17}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>2</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "							</clipitem>\n" +
    "						</track>\n" +
    "						<track>\n" +
    "							<clipitem id=\"{18}\" frameBlend=\"FALSE\">\n" +
    "								<masterclipid>{19}</masterclipid>\n" +
    "								<name>{20}</name>\n" +
    "								<file id=\"{21}\"/>\n" +
    "								<sourcetrack>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>2</trackindex>\n" +
    "								</sourcetrack>\n" +
    "								<link>\n" +
    "									<linkclipref>{22}</linkclipref>\n" +
    "									<mediatype>video</mediatype>\n" +
    "									<trackindex>1</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{23}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>1</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "								<link>\n" +
    "									<linkclipref>{24}</linkclipref>\n" +
    "									<mediatype>audio</mediatype>\n" +
    "									<trackindex>2</trackindex>\n" +
    "									<clipindex>1</clipindex>\n" +
    "									<groupindex>1</groupindex>\n" +
    "								</link>\n" +
    "							</clipitem>\n" +
    "						</track>\n" +
    "					</audio>\n" +
    "				</media>\n" +
    "				<logginginfo>\n" +
    "					<description></description>\n" +
    "					<scene></scene>\n" +
    "					<shottake></shottake>\n" +
    "					<lognote></lognote>\n" +
    "				</logginginfo>\n" +
    "			</clip>\n";

        string gXMLTail =
    "		</children>\n" +
    "	</project>\n" +
    "</xmeml>\n";

        ///
        ///
        ///////////////////////////////////////

        List<int> _durations;
        List<string> _paths;
        public FinalCutProXMLBuilder ()
        {
            _durations = new List<int>();
            _paths = new List<string>();
        }

        public List<int> GetDurations() { return _durations; }
        public List<string> GetPaths() { return _paths; }

        private void AddText(FileStream fs, string value)
        {
            byte[] info = new UTF8Encoding(true).GetBytes(value);
            fs.Write(info, 0, info.Length);
        }

        public int GetNumFrames(string movFile)
        {
            List<string> arrHeaders = new List<string>();
            List<int> arrIndeces = new List<int>();

            Shell32.Shell shell = new Shell32.Shell();
            Shell32.Folder objFolder;

            string fullPath = Path.GetFullPath(movFile);
            objFolder = shell.NameSpace(Path.GetDirectoryName(fullPath));


            for (int i = 0; i < short.MaxValue; i++)
            {
                string header = objFolder.GetDetailsOf(null, i);
                //if (String.IsNullOrEmpty(header))
                //    break;
                //arrHeaders.Add(header);
                if (!String.IsNullOrEmpty(header))
                {
                    arrHeaders.Add(header);
                    arrIndeces.Add(i);
                }
            }

            //foreach (Shell32.FolderItem2 item in objFolder.Items())
            Shell32.FolderItem item = objFolder.ParseName(Path.GetFileName((movFile)));
            if (true)
            {
                string lenString = "";
                int frameRate = 0;
                int secs = 0;
                int i = 0;
                for (i = 0; i < arrHeaders.Count; i++)
                {
                    //Console.WriteLine("{0}\t{1}: {2}", arrIndeces[i], arrHeaders[i], objFolder.GetDetailsOf(item, arrIndeces[i]));
                    if (arrIndeces[i] == 27) // length
                    {
                        lenString = objFolder.GetDetailsOf(item, arrIndeces[i]);
                        string[] lenTokens = lenString.Split(':');
                        secs = int.Parse(lenTokens[2]) + (60 * int.Parse(lenTokens[1])) + (60 * 60 * int.Parse(lenTokens[0]));
                    }
                    else if (arrIndeces[i] == 305) //frame rate
                    {
                        string fr = objFolder.GetDetailsOf(item, arrIndeces[i]);
                        string fr2 = fr.Substring(1);
                        int j = 0;
                        for (j = 0; j < fr2.Length; j++)
                        {
                            if ((fr2[j] < '0') || (fr2[j] > '9'))
                                break;
                        }
                        int val = 0;
                        int.TryParse(fr.Substring(1, j), out val);
                        frameRate = val;
                        //Console.WriteLine("frame rate {0}", val);
                    }
                }
                int frames = 0;
                switch (frameRate)
                {
                    case 29:
                        frames = (int)((float)secs * 29.97);
                        break;
                    case 59:
                        frames = (int)((float)secs * 59.94);
                        break;
                    default:
                        frames = (int)((float)secs * frameRate);
                        break;
                }
                return frames;
            }
        }

        //[STAThread]
        public void GenPluralEyesXML(string camDir, string xmlOut)
        {

            // get each cam clip name and duration
            // find a camera in the directory that ends with "_cam_01." and an appropriate movie file extension
            string camSearchName = "*_cam_??.*";
            string[] validMovieExtensions = { ".mp4", ".mov", ".avi" };
            if (!Directory.Exists(camDir))
            {
                Console.WriteLine("Could not find directory \"{0}\"", camDir);
                return;
            }
            string[] files = Directory.GetFiles(camDir, camSearchName);
            if (files.Length == 0)
            {
                Console.WriteLine("Could not find file with format {0} in directory {1}", camSearchName, camDir);
                return;
            }
            int numCameras = files.Length;
            string baseName = Path.GetFileNameWithoutExtension(files[0]);
            string camExtension = Path.GetExtension(files[0]);
            string camNameFmt = camDir + "\\" + baseName.Substring(0, baseName.Length - 2) + "{0:00}" + Path.GetExtension(files[0]);
            int duration = 0;
            for (int i = 0; i < numCameras; i++)
            {
                string camPath = string.Format(camNameFmt, i + 1);
                _paths.Add(camPath);

                int camDuration = GetNumFrames(camPath);
                _durations.Add(camDuration);

                if (i == 0)
                    duration = camDuration;
                else
                {
                    if (camDuration < duration)
                        duration = camDuration;
                }
            }

            Array.Sort<string>(files);
            for (int i = 0; i < files.Length; i++)
                Console.WriteLine("{0}: {1}", i, files[i]);

            //string path = camDir + "\\syncToPluralEyes.xml";
            string path = xmlOut ;
            if (File.Exists(path))
            {
                File.Delete(path);
            }
            using (FileStream fs = File.Create(path))
            {
                // build header of xml
                AddText(fs, gXMLHeader);

                string fmtedText = string.Format(gXMLHeader2, duration);
                AddText(fs, fmtedText);

                AddText(fs, gXMLHeader3);

                // build video tracks
                for (int i = 0; i < numCameras; i++)
                {
                    int camNum = i + 1;
                    string camName = string.Format("cam_{0:00}{1}", camNum, camExtension);
                    string urlStr = string.Format(camNameFmt, camNum);
                    urlStr = urlStr.Replace('\\', '/');
                    urlStr = "file://localhost/" + urlStr.Replace(":", "%3a");

                    fmtedText = string.Format(gXMLVideoFileTrack,
                        // 0 - "							<clipitem id=\"clipitem-2\" frameBlend=\"FALSE\">\n" +
                        "clipitem-" + camNum.ToString(),
                        // 1 - "								<masterclipid>masterclip-2</masterclipid>\n" +
                        "masterclip-" + camNum.ToString(),
                        // 2 - "								<name>Shot01_cam_02.MP4</name>\n" +
                        camName,
                        // 3 - "								<duration>1320</duration>\n" +
                        duration,
                        // 4 - "								<end>1320</end>\n" +
                        duration,
                        // 5 - "								<out>1320</out>\n" +
                        duration,
                        // 6 - "								<file id=\"file-2\">\n" +
                        "file-" + camNum.ToString(),
                        // 7 - "									<name>Shot01_cam_02.MP4</name>\n" +
                        camName,
                        // 8 - "									<pathurl>file://localhost/Z%3a/CreativeStudioJobs/VR_Flex360/08_TRANSFER/EXTERNAL/SHOOT/Flexi_20160728/SortedCameras/Shot_01/Shot01_cam_02.MP4</pathurl>\n" +
                        urlStr,
                        // 9 - "									<duration>1320</duration>\n" +
                        duration,
                        // 10 - "									<linkclipref>clipitem-2</linkclipref>\n" +
                        "clipitem-" + camNum.ToString(),
                        // 11 - "									<trackindex>2</trackindex>\n" +
                        camNum,
                        // 12 - "									<linkclipref>clipitem-8</linkclipref>\n" +
                        "clipitem-" + (numCameras + 1 + (i * 2)).ToString(),
                        // 13 - "									<trackindex>3</trackindex>\n" +
                        ((i * 2) + 1).ToString(),
                        // 14 - "									<linkclipref>clipitem-9</linkclipref>\n" +
                        "clipitem-" + (numCameras + 1 + (i * 2) + 1).ToString(),
                        // 15 - "									<trackindex>4</trackindex>\n" +
                        ((i * 2) + 2).ToString());
                    AddText(fs, fmtedText);
                }

                AddText(fs, gXMLVideoToAudio);

                // build audio tracks
                for (int i = 0; i < numCameras; i++)
                {
                    int camNum = i + 1;
                    string camName = string.Format("cam_{0:00}{1}", camNum, camExtension);

                    // right
                    fmtedText = string.Format(gXMLAudioTrack,
                    // 0 - "							<clipitem id=\"clipitem-8\" frameBlend=\"FALSE\" PannerCurrentValue=\"0.5\" PannerKeyframes=\"\" PannerStartKeyframe=\"-91445760000000000,0.5,0,0,0,0,0,0\" PannerIsInverted=\"true\" PannerName=\"Balance\">\n" +
                    "clipitem-" + (numCameras + (i * 2) + 1).ToString(),
                    // 1 - "								<masterclipid>masterclip-2</masterclipid>\n" +
                    "masterclip-" + camNum.ToString(),
                    // 2 - "								<name>Shot01_cam_02.MP4</name>\n" +
                    camName,
                    // 3 - "								<duration>1320</duration>\n" +
                    duration,
                    // 4 - "								<end>1320</end>\n" +
                    duration,
                    // 5 - "								<out>1320</out>\n" +
                    duration,
                    // 6 - "								<file id=\"file-2\"/>\n" +
                    "file-" + camNum.ToString(),
                    // 7 - "									<linkclipref>clipitem-2</linkclipref>\n" +
                    "clipitem-" + camNum.ToString(),
                    // 8 - "									<trackindex>{8}</trackindex>\n" +
                    camNum,
                    // 9 - "									<linkclipref>clipitem-8</linkclipref>\n" +
                    "clipitem-" + (numCameras + 1 + (i * 2)).ToString(),
                    // 10 - "									<trackindex>3</trackindex>\n" +
                    ((i * 2) + 1).ToString(),
                    // 11 - "									<linkclipref>clipitem-9</linkclipref>\n" +
                    "clipitem-" + (numCameras + 1 + (i * 2) + 1).ToString(),
                    // 12 - "									<trackindex>4</trackindex>\n" +
                    ((i * 2) + 2).ToString(),
                    // 13 - 1 for first, 2 for 2nd
                    1,
                    // 14 - 1 for first, 2 for 2nd
                    1);
                    AddText(fs, fmtedText);

                    // left
                    fmtedText = string.Format(gXMLAudioTrack,
                    // 0 - "							<clipitem id=\"clipitem-8\" frameBlend=\"FALSE\" PannerCurrentValue=\"0.5\" PannerKeyframes=\"\" PannerStartKeyframe=\"-91445760000000000,0.5,0,0,0,0,0,0\" PannerIsInverted=\"true\" PannerName=\"Balance\">\n" +
                    "clipitem-" + (numCameras + (i * 2) + 2).ToString(),
                    // 1 - "								<masterclipid>masterclip-2</masterclipid>\n" +
                    "masterclip-" + camNum.ToString(),
                    // 2 - "								<name>Shot01_cam_02.MP4</name>\n" +
                    camName,
                    // 3 - "								<duration>1320</duration>\n" +
                    duration,
                    // 4 - "								<end>1320</end>\n" +
                    duration,
                    // 5 - "								<out>1320</out>\n" +
                    duration,
                    // 6 - "								<file id=\"file-2\"/>\n" +
                    "file-" + camNum.ToString(),
                    // 7 - "									<linkclipref>clipitem-2</linkclipref>\n" +
                    "clipitem-" + camNum.ToString(),
                    // 8 - "									<trackindex>{8}</trackindex>\n" +
                    camNum,
                    // 9 - "									<linkclipref>clipitem-8</linkclipref>\n" +
                    "clipitem-" + (numCameras + 1 + (i * 2)).ToString(),
                    // 10 - "									<trackindex>3</trackindex>\n" +
                    ((i * 2) + 1).ToString(),
                    // 11 - "									<linkclipref>clipitem-9</linkclipref>\n" +
                    "clipitem-" + (numCameras + 1 + (i * 2) + 1).ToString(),
                    // 12 - "									<trackindex>4</trackindex>\n" +
                    ((i * 2) + 2).ToString(),
                    // 13 - 1 for first, 2 for 2nd
                    2,
                    // 14 - 1 for first, 2 for 2nd
                    2);
                    AddText(fs, fmtedText);
                }

                AddText(fs, gXMLAudioToList);

                // build clip list
                for (int i = 0; i < numCameras; i++)
                {
                    int camNum = i + 1;
                    string camName = string.Format("cam_{0:00}{1}", camNum, camExtension);

                    fmtedText = string.Format(gXMLClipList,
                        // 0 - "			<clip id=\"masterclip-2\" frameBlend=\"FALSE\">\n" +
                        "masterclip-" + camNum.ToString(),
                        // 1 - "				<masterclipid>masterclip-2</masterclipid>\n" +
                        "masterclip-" + camNum.ToString(),
                        // 2 - "				<duration>1320</duration>\n" +
                        duration,
                        // 3 - "				<name>Shot01_cam_02.MP4</name>\n" +
                        camName,
                        // 4 - "							<clipitem id=\"clipitem-19\" frameBlend=\"FALSE\">\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 1).ToString(),
                        // 5 - "								<masterclipid>masterclip-2</masterclipid>\n" +
                        "masterclip-" + camNum.ToString(),
                        // 6 - "								<name>Shot01_cam_02.MP4</name>\n" +
                        camName,
                        // 7 - "								<file id=\"file-2\"/>\n" +
                        "file-" + camNum.ToString(),
                        // 8 - "									<linkclipref>clipitem-19</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 1).ToString(),
                        // 9 - "									<linkclipref>clipitem-20</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 2).ToString(),
                        // 10 - "									<linkclipref>clipitem-21</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 3).ToString(),
                        // 11 - "							<clipitem id=\"clipitem-20\" frameBlend=\"FALSE\">\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 2).ToString(),
                        // 12 - "								<masterclipid>masterclip-2</masterclipid>\n" +
                        "masterclip-" + camNum.ToString(),
                        // 13 - "								<name>Shot01_cam_02.MP4</name>\n" +
                        camName,
                        // 14 - "								<file id=\"file-2\"/>\n" +
                        "file-" + camNum.ToString(),
                        // 15 - "									<linkclipref>clipitem-19</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 1).ToString(),
                        // 16 - "									<linkclipref>clipitem-20</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 2).ToString(),
                        // 17 - "									<linkclipref>clipitem-21</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 3).ToString(),
                        // 18 - "							<clipitem id=\"clipitem-21\" frameBlend=\"FALSE\">\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 3).ToString(),
                        // 19 - "								<masterclipid>masterclip-2</masterclipid>\n" +
                        "masterclip-" + camNum.ToString(),
                        // 20 - "								<name>Shot01_cam_02.MP4</name>\n" +
                        camName,
                        // 21 - "								<file id=\"file-2\"/>\n" +
                        "file-" + camNum.ToString(),
                        // 22 - "									<linkclipref>clipitem-19</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 1).ToString(),
                        // 23 - "									<linkclipref>clipitem-20</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 2).ToString(),
                        // 24 - "									<linkclipref>clipitem-21</linkclipref>\n" +
                        "clipitem-" + (numCameras + (2 * numCameras) + (i * 3) + 3).ToString());
                    AddText(fs, fmtedText);
                }

                AddText(fs, gXMLTail);
                fs.Close();
            }
        }
    }
}