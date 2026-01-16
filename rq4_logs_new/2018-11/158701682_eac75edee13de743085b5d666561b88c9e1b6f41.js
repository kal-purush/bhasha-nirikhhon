var app = angular.module('myApp', [
        'ngSanitize',
        'ngAnimate',
        'ngQuantum',
        ]);


// :: MAIN CONTROLLER
app.controller('MainCtrl',
	function($scope){

	    $scope.buttons = { radio: {what: null}};

	    $scope.request_params = {
                project_type: null,
                n_samples: null,
                target_regions: null,
                average_coverage: null,
                sequencer: null,
                n_lanes: null,
                single_paired: null,
                reads_length: null,
                custom_reads_length: null,
                first_name: null,
                last_name: null,
                analysis: null,
                str_analysis: null,
                your_request: null,
        }

	    $scope.project_types = [
            {id: 'DNA', title: 'DNA (Whole Genome)'},
            {id: 'M-RNA', title: 'mRNA (polyA-selected)'},
            {id: 'TOT-RNA', title: 'Total RNA'},
            {id: 'S-RNA', title: 'Small RNA (micro RNA)'},
            {id: 'EXOME', title: 'Exome'},
            {id: 'GENE', title: 'Gene Panel'},
        ];

        $scope.sequencers = [
            {id: '2500', title: 'HiSeq 2500'},
            {id: '3000', title: 'HiSeq 3000'},
            {id: 'MYSEQ', title: 'MySeq'},
        ];

        $scope.lengths = [
            {id: '50', title: '50'},
            {id: '75', title: '75'},
            {id: '100', title: '100'},
            {id: '150', title: '150'},
            {id: '250', title: '250'},
            {id: '0', title: 'Other'},
        ];

        $scope.patch =  {
                incipit: "I need XXX ",
                for_samples: "for XXX samples ",
                coverage: "with an average coverage of XXXx ",
                target_regions: "on targeted regions of XXX mbases ",
                min_num_reads: "with at least XXX reads per sample ",
                project: "for a XXX project",
                for_lanes: "for XXX lanes ",
                sequencer: "with XXX Illumina Sequencer ",
                ends: "in XXX ends ",
                reads_length: " and read length equal to XXX ",
                analysis: "\n\nPlease include data analysis: \n\n XXX"
        }

        $scope.patchwork = JSON.parse(JSON.stringify($scope.patch));


        $scope.build_request = function (params) {

	         _request = null;
	        if ($scope.buttons.radio.what === 'all') {
	            _request = $scope.patchwork.incipit + $scope.patchwork.for_samples;

	            switch (params.project_type.id)
                {
                    case "DNA":
                    case "EXOME":
                    case "GENE":
                        _request = _request + $scope.patchwork.coverage + $scope.patchwork.target_regions;
                        break;

                    case "M-RNA":
                    case "TOT-RNA":
                    case "S-RNA":
                       _request = _request + $scope.patchwork.min_num_reads;
                       break;
                }
                _request = _request + $scope.patchwork.project;

                if (params.analysis===true) {
                    _request = _request + $scope.patchwork.analysis;
                }
	        }
	        else {
	            _request = $scope.patchwork.incipit + $scope.patchwork.for_lanes + $scope.patchwork.sequencer + $scope.patchwork.ends + $scope.patchwork.reads_length;
	        }


	        return _request;
	    }



        $scope.init_request = function() {

            $scope.buttons = { radio: {what: 'all'}};

            this.request = {
                project_type: $scope.project_types[0],
                n_samples: 1,
//                target_regions: '3,000,000',
//                average_coverage: 12,
//                min_num_reads: ''
                sequencer: $scope.sequencers[0],
                n_lanes: 1,
                single_paired: false,
                reads_length: $scope.lengths[0],
                custom_reads_length: 300,
                first_name: null,
                last_name: null,
                analysis: false,
                str_analysis: null,
                your_request: null,
            }

            if ($scope.buttons.radio.what === 'all') {
                $scope.patchwork.incipit = $scope.patch.incipit.replace('XXX', "library preparation and sequencing");

            }
            else {
                $scope.patchwork.incipit = $scope.patch.incipit.replace('XXX', "sequencing of pre-constructed libraries");
            }


            switch (this.request.project_type.id)
            {
                case "DNA":
                    this.request.average_coverage = '30';
                    this.request.target_regions = '3,000,000,000';
                    this.request.str_analysis = "SNP and INDEL calling\n\nAdvanced analysis including annotations (allele frequencies in reference datasets - ExAC, 1000Genome, ESP6500; predicted pathogenicity; functional annotations at transcript level with multiple databases - UCSC, Ensembl, RefGene, Gencode) and genetic inheritance\n\nAdditional analysis are available: CNV, extended annotations for non-coding regions\n\nCustom population scale analysis are available\n\nRemote access to alignments for visual inspection using IGV\n\nAnalysis results are delivered through our web-based platform\n\nAdditionally raw data are available via secure cloud"
                    break;
                case "EXOME":
                case "GENE":
                    this.request.average_coverage = '70';
                    this.request.target_regions = '40,000,000,000';
                    this.request.str_analysis = "SNP and INDEL calling\n\nAdvanced analysis including annotations (allele frequencies in reference datasets - ExAC, 1000Genome, ESP6500; predicted pathogenicity; functional annotations at transcript level with multiple databases - UCSC, Ensembl, RefGene, Gencode) and genetic inheritance\n\nAdditional analysis are available: CNV, extended annotations for non-coding regions\n\nCustom population scale analysis are available\n\nRemote access to alignments for visual inspection using IGV\n\nAnalysis results are delivered through our web-based platform\n\nAdditionally raw data are available via secure cloud"
                    break;
                case "M-RNA":
                case "TOT-RNA":
                    this.request.min_num_reads = '30,000,000';
                    this.request.str_analysis = 'Normalized transcript expression\n\nIdentification of differentially expressed genes in different conditions\n\nAdvanced analysis including gene set enrichment analysis for the identification of pathway perturbations\n\nRemote access to alignments for visual inspection using IGV\n\nAnalysis results are delivered through our web-based platform\n\nAdditionally raw data are available via secure cloud'
                    break;
                case "S-RNA":
                   this.request.min_num_reads = '10,000,000';
                   this.request.str_analysis = "smallRNA quantification and de-novo identification of miRNAs\n\nIdentification of differentially expressed miRNAs\n\nAnalysis results are delivered through our web-based platform\n\nAdditionally raw data are available via secure cloud"
                   break;
            }

            $scope.patchwork.target_regions = $scope.patch.target_regions.replace('XXX', this.request.target_regions);
            $scope.patchwork.project = $scope.patch.project.replace('XXX', this.request.project_type.title);
            $scope.patchwork.for_samples = $scope.patch.for_samples.replace('XXX', this.request.n_samples);
            $scope.patchwork.for_lanes = $scope.patch.for_lanes.replace('XXX', this.request.n_lanes);
            $scope.patchwork.coverage = $scope.patch.coverage.replace('XXX', this.request.average_coverage);
            $scope.patchwork.sequencer = $scope.patch.sequencer.replace('XXX', this.request.sequencer.title);
            $scope.patchwork.analysis = $scope.patch.analysis.replace('XXX', this.request.str_analysis);

            if (this.request.reads_length.id !== '0') {
                $scope.patchwork.reads_length = $scope.patch.reads_length.replace('XXX', this.request.reads_length.title);
            }
            else {
                $scope.patchwork.reads_length =  $scope.patch.reads_length.replace('XXX', this.request.custom_reads_length);
            }

            if (this.request.single_paired) {
                $scope.patchwork.ends = $scope.patch.ends.replace('XXX', "Paired");
            }
            else {
                $scope.patchwork.ends = $scope.patch.ends.replace('XXX', "Single");
            }

            this.request.your_request = $scope.build_request(this.request);

            return this.request;
        }

        $scope.$watch('buttons.radio.what',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                if (newValue === 'all') {
                    $scope.patchwork.incipit = $scope.patch.incipit.replace('XXX', "library preparation and sequencing");

                }
                else {
                    $scope.patchwork.incipit = $scope.patch.incipit.replace('XXX', "sequencing of pre-constructed libraries");
                }
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.sequencer',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                $scope.patchwork.sequencer = $scope.patch.sequencer.replace('XXX', newValue.title);
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.single_paired',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                if (newValue) {
                    $scope.patchwork.ends = $scope.patch.ends.replace('XXX', "Paired");
                }
                else {
                    $scope.patchwork.ends = $scope.patch.ends.replace('XXX', "Single");
                }
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.analysis',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}

                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.reads_length',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                if (newValue.id !== '0') {
                    $scope.patchwork.reads_length = $scope.patch.reads_length.replace('XXX', newValue.title);
                }
                else {
                    $scope.patchwork.reads_length =  $scope.patch.reads_length.replace('XXX', $scope.request_params.custom_reads_length);
                }
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.custom_reads_length',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                $scope.patchwork.reads_length =  $scope.patch.reads_length.replace('XXX', $scope.request_params.custom_reads_length);
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.project_type',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                $scope.patchwork.project = $scope.patch.project.replace('XXX', newValue.title);
                switch (newValue.id)
                {
                    case "DNA":
                        $scope.request_params.average_coverage = '30';
                        $scope.request_params.target_regions = '3,000,000,000';
                        $scope.request_params.str_analysis = "SNP and INDEL calling\n\nAdvanced analysis including annotations (allele frequencies in reference datasets - ExAC, 1000Genome, ESP6500; predicted pathogenicity; functional annotations at transcript level with multiple databases - UCSC, Ensembl, RefGene, Gencode) and genetic inheritance\n\nAdditional analysis are available: CNV, extended annotations for non-coding regions\n\nCustom population scale analysis are available\n\nRemote access to alignments for visual inspection using IGV\n\nAnalysis results are delivered through our web-based platform\n\nAdditionally raw data are available via secure cloud"

                        break;
                    case "EXOME":
                    case "GENE":
                        $scope.request_params.average_coverage = '70';
                        $scope.request_params.target_regions = '40,000,000,000';
                        $scope.request_params.str_analysis = "SNP and INDEL calling\n\nAdvanced analysis including annotations (allele frequencies in reference datasets - ExAC, 1000Genome, ESP6500; predicted pathogenicity; functional annotations at transcript level with multiple databases - UCSC, Ensembl, RefGene, Gencode) and genetic inheritance\n\nAdditional analysis are available: CNV, extended annotations for non-coding regions\n\nCustom population scale analysis are available\n\nRemote access to alignments for visual inspection using IGV\n\nAnalysis results are delivered through our web-based platform\n\nAdditionally raw data are available via secure cloud"
                        break;
                    case "M-RNA":
                    case "TOT-RNA":
                        $scope.request_params.min_num_reads = '30,000,000';
                        $scope.request_params.str_analysis = 'Normalized transcript expression\n\nIdentification of differentially expressed genes in different conditions\n\nAdvanced analysis including gene set enrichment analysis for the identification of pathway perturbations\n\nRemote access to alignments for visual inspection using IGV\n\nAnalysis results are delivered through our web-based platform\n\nAdditionally raw data are available via secure cloud'
                        break;
                    case "S-RNA":
                       $scope.request_params.min_num_reads = '10,000,000';
                       $scope.request_params.str_analysis = "smallRNA quantification and de-novo identification of miRNAs\n\nIdentification of differentially expressed miRNAs\n\nAnalysis results are delivered through our web-based platform\n\nAdditionally raw data are available via secure cloud"
                       break;
                }
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.n_samples',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                $scope.patchwork.for_samples = $scope.patch.for_samples.replace('XXX', newValue);
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.n_lanes',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                $scope.patchwork.for_lanes = $scope.patch.for_lanes.replace('XXX', newValue);
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.average_coverage',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                $scope.patchwork.coverage = $scope.patch.coverage.replace('XXX', newValue);
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

         $scope.$watch('request_params.target_regions',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                $scope.patchwork.target_regions = $scope.patch.target_regions.replace('XXX', newValue);
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.$watch('request_params.min_num_reads',
            function (newValue, oldValue) {
                if (newValue === null || newValue === oldValue) {return;}
                $scope.patchwork.min_num_reads = $scope.patch.min_num_reads.replace('XXX', newValue);
                $scope.request_params.your_request = $scope.build_request($scope.request_params);
            }
        );

        $scope.request_params = $scope.init_request();

});