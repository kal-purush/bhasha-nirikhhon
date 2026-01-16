using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using TSVUtility;

namespace DeepTermWeightFeatureCalculator
{
    class Program
    {
        static void Main(string[] args)
        {
            string inputExtraction = args[0];
            string outputExtraction = args[1];
            string word2vecBinaryModel = args[2];
            Dictionary<String, float[]> model = LoadWord2vecModel(word2vecBinaryModel);
            using (StreamReader sr = new StreamReader(TSVFile.OpenInputTSVStream(inputExtraction)))
            using (StreamWriter sw = new StreamWriter(TSVFile.OpenOutputTSVStream(outputExtraction,false)))
            {
                double[] feature_weight = new double[300] {0.2538461826219447, -0.0686701626700071, -0.09006037319865194, 0.010241813154946948, 0.16611995837541962, -0.3200690030466925, -0.05746840591717508, -0.1927495240028911, -0.22660920941591495, -0.10381030156269831, -0.14736031889966342, 0.11536268609662927, 0.12119522536615442, -0.039806620784286324, -0.0025101378952937688, -0.2153229411692349, -0.06793695499553809, -0.19982183784820795, 0.1785592770014214, -0.03728167079366318, -0.04866069425856831, -0.05480345738563925, 0.03806922005174796, -0.18659513727092658, 0.035261132976043434, 0.09538052854804377, 0.06910223780419475, -0.026332687914170854, 0.0009809842277506747, 0.1347575137793603, -0.0681431589269305, -0.03505422921691469, 0.04904192967538813, 0.1599604306864603, 0.05009272261731429, -0.1258245585154701, -0.12267849745633382, -0.06445273569908316, 0.04455768851052863, 0.1456388688807849, -0.057093726230681846, -0.12936979341629265, -0.13638922771686002, -0.08939319319834929, -0.011476289596134033, 0.15948959216655081, 0.18045344609112682, 0.1715445691915844, 0.11689097401556907, 0.00357715055288158, 0.1033659184460083, -0.13710154342921194, -0.13340939222299508, -0.13417341504660002, 0.02397604688687353, 0.08238485079744025, 0.05072189657209351, 0.24972006746824357, -0.04953889910780581, 0.1987699083332565, -0.06863651671206697, 0.0708081998661408, -0.01772202917173831, -0.17708075170242213, 0.1092504029870327, -0.1615226924410854, -0.002824149896208, -0.15871479301424538, 0.09551884065988868, 0.22499299478770737, 0.042810565305358976, -0.12060475691977857, -0.1649414248444354, 0.0950155892901811, -0.007700405282429591, -0.14405241847707634, 0.09016637986593633, 0.0, 0.03517828854534869, 0.08261696667192023, -0.012270322184217588, -0.14966906300820781, 0.049029023162959526, -0.146348688381927, -0.10657611733470904, -0.10655303765811588, 0.20842227542584651, 0.21350690259445912, -0.19801641517204105, -0.20697618723363664, -0.08597897404990515, 0.05420691069740911, -0.11987485077792477, -0.19602894653596667, -0.30559499902745363, 0.04073064278074961, 0.2212026667741698, -0.05832500695868755, -0.10394692395991723, 0.3695558830689171, -0.005793209310414317, 0.01903310756683418, 0.11051303508366477, -0.10246715824550284, -0.03186798882681539, 0.2014930754053219, -0.10764951320539465, 0.11619974102593565, -0.06571329645418186, 0.05213321854838495, -0.1451942065295862, -0.0031139050846557895, -0.021807048007396152, 0.06812344103068556, -0.31649584251145874, 0.20166552377872646, 0.2216094587519973, -0.13333208957167494, -0.10996714815341177, -0.1571511611578555, 0.011146248503686357, -0.21279734217335172, -0.3086844176648976, 0.13960434294636065, -0.17544726875147848, -0.26752957670639677, -0.18539788931921058, 0.12403277495848146, -0.13146737000714312, 0.045057415657836365, 0.09549312508753022, 0.1282164084042249, -0.1852291525033577, -0.12363997752903899, 0.025274005243707477, -0.1432111161966799, 0.028109335662460138, -0.19241225973649792, 0.25063543765733615, -0.024721741876937833, 0.06764354492418954, -0.029344067911690565, -0.03026715835880135, -0.2206575941082282, -0.21532023620068988, 0.031279984478324654, 0.21756237100709266, 0.09470035331599212, -0.07611208412946659, 0.09019794038772162, -0.1083477012384884, -0.17691437850337238, -0.1879493155919005, -0.006056503489002652, -0.1404576364445292, 0.11202378571327978, 0.04439694489311643, -0.16093301008062177, 0.03988707430368873, -0.0015753594664960938, -0.04629162013281132, 0.06962310554652107, 0.0533513404393514, 0.15868486162453477, -0.02931484289440335, 0.009453368678482111, -0.038394404037557554, 0.061849554088233484, 0.0, -0.006642123233885528, 0.008869541483276837, -0.2053017895318143, -0.20969205915119835, -0.010971307237947456, -0.09785662435795048, -0.23031486653075323, 0.05387902285400744, -0.03590919473861954, 0.20686936388824181, -0.11193556823296877, 0.023196225103504914, 0.14765682560593255, -0.2612241571497319, 0.13992584571874067, -0.00890802236698661, -0.10109580710689492, 0.1433220484486877, -0.2785484734344504, 0.23486021428980686, -0.05438133348268732, -0.08227834239204994, 0.262134353420296, 0.039216623584098435, 0.29641728240268195, 0.18816167568582823, -0.009999733229932221, -0.029847182257469417, 0.034313840286451604, -0.16388570990350332, -0.035028095836567304, 0.003648822232978713, 0.055995982015074844, -0.015422474389861099, 0.2317731051717546, 0.0042360297926754605, 0.018260764542252533, 0.06469089965819264, 0.18536546202138415, 0.06514611562185264, -0.00239938170349781, 0.03314845714693998, -0.05145930970774032, -0.01087456297094482, -0.03217405832674326, -0.018547721106957266, 0.13324243317501508, -0.2305852610248915, -0.1545052789766213, 0.08686151415013053, -0.020826343844124167, 0.06525417114290195, -0.08861328325945618, 0.06215810645144515, -0.17174651019914483, -0.0715576951991129, 0.014493799522224881, 0.011214157751636698, 0.12174100394428665, 0.049637394007593545, 0.18049744595730924, 0.005001785036087733, -0.09329506938347155, 0.18559212312749554, -0.15239136908391027, -0.004325657683385898, 0.012133537097089604, 0.036038759959274545, 0.10471249983988289, -0.04725311662558393, 0.20631377417775082, -0.07397148558402832, -0.01047857915026429, 0.0645852939696661, -0.033278644546580284, 0.04357401529335759, -0.31183745887279035, 0.17123628011259803, 0.1818856907218545, -0.09675749934956455, 0.188981562180895, -0.08189464986182685, -0.03936709371880006, 0.14385154415969278, 0.0711579375736589, -0.06888724935373179, 0.04168909711619999, -0.01258340882825076, 0.05300427771251361, 0.11186829284302975, 0.11895481404113507, 0.03375407989670895, 0.19704550656217293, 0.07172560346412016, 0.03141101182352664, 0.08349284322149193, -0.14155144458842578, 0.07030418504419357, 0.07908101773813546, 0.2723930142314312, 0.13541112124844049, 0.07688919954539378, -0.008367473340056695, -0.035704947828022575, 0.29221989404594206, 0.037628554097414224, -0.0981772269673222, 0.2817900398981206, -0.05345188651367586, -0.013043812239508066, -0.2334262446413108, 0.150450791729621, -0.08971941701284769, -0.13387407121713765, 0.04619728510762145, -0.11815924390613752, 0.15364599731826173, 0.048670112770281666, -0.12594078551832524, -0.020983153233194673, -0.034821815325370174, -0.1715868593419294, -0.0792120422152978, 0.16840214358196495, 0.11721076757088779, 0.06484404805630939, 0.05906618529852858, -0.3429377835341666, -0.09679992436156105, 0.2426323808019555, -0.02137471106044561};
                TSVReader tsvReader = new TSVReader(sr, true, false);
                // write new header
                String header = tsvReader.GetHeaderLine();
                Boolean has_title_snippet = false;
                if (header.Contains("\tTitle\t")) has_title_snippet = true;
                header = header.Replace("Title\tSnippet\t", "");
                header += "\ttermweight_wordsfound";
                sw.WriteLine(header);
                TSVLine line;
                int count1 = 0;
                while ((line = tsvReader.ReadLine()) != null)
                {
                    count1++;
                    Dictionary<String, Double> term_weight_map = new Dictionary<String, Double>();
                    String query = line.GetFeatureValueString("Query");
                    String passage = line.GetFeatureValueString("Passage");
                    
                    String[] terms = query.Split(' ');
                    
                    // calculate wordvec of every term
                    Dictionary<String, float[]> term_word2vec = new Dictionary<String, float[]>();
                    for (int i = 0; i < terms.Length; i++)
                    {
                        String term = terms[i];
                        if (model.ContainsKey(term))
                        {
                            float[] vec = model[term];
                            if (!term_word2vec.ContainsKey(term))
                            {
                                term_word2vec.Add(term, vec);
                            }
                            
                        }
                    }
                    // calculate average word2vec
                    
                    float[] average = new float[300];
                    int count = 0;
                    foreach (String temp_term in term_word2vec.Keys)
                    {
                        float[] vec = term_word2vec[temp_term];
                        for (int i = 0; i < 300; i++)
                        {
                            average[i] += vec[i];
                        }
                        count++;
                    }
                    if (count > 0)
                    {
                        for (int i = 0; i < 300; i++)
                        {
                            average[i] /= count;
                        }
                    }
                    // calculate relative term word2vec to average
                    Dictionary<String, float[]> term_feature_vector = new Dictionary<String, float[]>();
                    foreach (String temp_term in term_word2vec.Keys)
                    {
                        float[] vec = term_word2vec[temp_term];
                        for (int i = 0; i < 300; i++)
                        {
                            vec[i] -= average[i];
                        }
                        term_feature_vector.Add(temp_term, vec);
                    }
                    // get term weight
                    foreach (String temp_term in term_feature_vector.Keys)
                    {
                        float[] vec = term_feature_vector[temp_term];
                        double termweight = 0;
                        for (int i = 0; i < 300; i++)
                        {
                            termweight += vec[i] * feature_weight[i];
                        }
                        termweight = Math.Exp(termweight)/(1+ Math.Exp(termweight));
                        term_weight_map.Add(temp_term, termweight);
                    }
                    // normalize the term weight : sum is 1
                    double sum = 0;
                    int all_count = 0;
                    foreach(String temp_term in term_weight_map.Keys)
                    {
                        sum += term_weight_map[temp_term];
                    }
                    if (sum == 0) sum = 1;
                    Dictionary<String,double> normlized_term_weight_map = new Dictionary<String, double>();
                    if (sum != 0)
                    {
                        foreach (String temp_term in term_weight_map.Keys)
                        {
                            double normalize_termweight = term_weight_map[temp_term];
                            normalize_termweight /= sum;
                            normlized_term_weight_map.Add(temp_term, normalize_termweight);
                        }
                        
                    }
                   
                    String term_weight_str = "{";
                    foreach (String temp_term in term_weight_map.Keys)
                    {
                        term_weight_str += temp_term;
                        term_weight_str += ":";
                        term_weight_str += term_weight_map[temp_term];
                        term_weight_str += ",";
                    }
                    if (term_weight_str.EndsWith(","))
                    {
                        term_weight_str = term_weight_str.Remove(term_weight_str.Length - 1, 1);
                    }
                    term_weight_str += "}";

                    String[] lineitems = line.GetWholeLineString().Split('\t');
                    
                    String out_line = "";
                    for(int j = 0; j < lineitems.Length; j++)
                    {
                        if (has_title_snippet) {
                           if (j == 2 || j == 3) continue;
                        }
                        out_line += lineitems[j];
                        if (j < (lineitems.Length - 1)) out_line += "\t";

                    }
                    //out_line += "\t"+ term_weight_str;
                    int wordsfound = GetTermWeightWordsfound(query, term_weight_map, passage);
                    
                    out_line += "\t" + wordsfound;
                    sw.WriteLine(out_line);
                }
            }


        }
        static int GetTermWeight
        static int GetTermWeightWordsfound(String query, Dictionary<String, double> term_weight_map, String passage)
        {
            //if cannot find a weight of term, use max term weight for unfound terms because they are special
            double max = 0;
            foreach(double weight in term_weight_map.Values)
            {
                max = max > weight ? max : weight;
            }
            if (max == 0) max = 1;
            String[] terms = query.Split(' ');
            if (passage.Contains(','))
            {
                passage = passage.Replace(',',' ');
            }
            if (passage.Contains('.'))
            {
                passage = passage.Replace('.', ' ');
            }
            
            String[] passage_terms = passage.Split(' ');
            double wordsfound = 0;
            for(int i = 0; i < terms.Length; i++)
            {
                for(int j = 0; j < passage_terms.Length; j++)
                {
                    if (terms[i].Equals(passage_terms[j]))
                    {
                        if (term_weight_map.ContainsKey(terms[i]))
                        {
                            wordsfound += 1000* term_weight_map[terms[i]];
                        }else
                        {
                            wordsfound += 1000 * max;
                        }
                    }
                }
            }
            int ret = (int)wordsfound;
            if (ret < 0) ret = 0;
            return ret;
        }
        static Dictionary<String, float[]> LoadWord2vecModel(String binaryModelFile)
        {
            Dictionary<String, float[]> model = new Dictionary<string, float[]>();
            using (BinaryReader br = new BinaryReader(new FileStream(binaryModelFile,
                FileMode.Open), Encoding.ASCII))
            {
                //read the words and size in first line

                Int64 words = 0;
                Int64 size = 0;
                while (true)
                {
                    Char c = br.ReadChar();
                    if (c != ' ')
                    {
                        words = words * 10 + (c - '0');
                    }
                    else
                    {
                        break;
                    }
                }
               

                while (true)
                {
                    char c = br.ReadChar();
                    if (c != '\n')
                    {
                        size = size * 10 + (c - '0');
                    }
                    else
                    {
                        break;
                    }
                }
                for (int i = 0; i < words; i++)
                {
                    //if (i < 1000000) continue;
                    String word = "";
                    while (true)
                    {
                        char c = br.ReadChar();
                        if (c != 32)
                        {
                            word += c;
                        }else {
                            break;
                        }
                    }
                    float[] f = new float[size];
                    for(int j = 0; j < size; j++){
                        f[j] = br.ReadSingle();
                    }
                    if (!model.ContainsKey(word))
                    {
                        model.Add(word, f);
                    }
                    
                }
              
            }
            return model;
        }
    }
}