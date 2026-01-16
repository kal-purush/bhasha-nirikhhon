import { changeOption, filterFiles } from "./browseFilesUtil";

const files = [
    {
        _id: "1",
        data_format: "FASTQ",
        experimental_strategy: "WES",
        file_name: "cimac-6521-001.fa",
        number_of_samples: 1,
        file_size: 234,
        trial_name: "DFCI-1234",
        date_created: "2019-01-17T21:27:53.175496",
        gs_uri: "test_uri",
        download_link: "download_url",
        fastq_properties: null
    },
    {
        _id: "2",
        data_format: "FASTQ",
        experimental_strategy: "WES",
        file_name: "cimac-6521-002.fa",
        number_of_samples: 1,
        file_size: 21,
        trial_name: "DFCI-1234",
        date_created: "2019-01-17T21:27:53.175496",
        gs_uri: "test_uri",
        download_link: "download_url",
        fastq_properties: null
    },
    {
        _id: "3",
        data_format: "FASTQ",
        experimental_strategy: "WES",
        file_name: "cimac-6521-003.fa",
        number_of_samples: 1,
        file_size: 22345,
        trial_name: "DFCI-1234",
        date_created: "2019-01-17T21:27:53.175496",
        gs_uri: "test_uri",
        download_link: "download_url",
        fastq_properties: null
    },
    {
        _id: "4",
        data_format: "FASTQ",
        experimental_strategy: "WES",
        file_name: "cimac-6521-004.fa",
        number_of_samples: 1,
        file_size: 12345545,
        trial_name: "DFCI-1234",
        date_created: "2019-01-17T21:27:53.175496",
        gs_uri: "test_uri",
        download_link: "download_url",
        fastq_properties: null
    },
    {
        _id: "5",
        data_format: "VCF",
        experimental_strategy: "WES",
        file_name: "cimac-6521.vcf",
        number_of_samples: 1,
        file_size: 7654645,
        trial_name: "DFCI-9999",
        date_created: "2019-01-17T21:27:53.175496",
        gs_uri: "test_uri",
        download_link: "download_url",
        fastq_properties: null
    },
    {
        _id: "6",
        data_format: "MAF",
        experimental_strategy: "WES",
        file_name: "dfci-9999.maf",
        number_of_samples: 276,
        file_size: 1234567,
        trial_name: "DFCI-9999",
        date_created: "2019-01-17T21:27:53.175496",
        gs_uri: "test_uri",
        download_link: "download_url",
        fastq_properties: null
    }
];

test("Returns the same file list if nothing is filtered", () => {
    expect(filterFiles(files, [], [], [], "")).toEqual(files);
});

test("Filters it correctly", () => {
    expect(
        filterFiles(files, ["DFCI-9999"], ["WES"], ["MAF", "VCF"], "dfci")
    ).toEqual([
        {
            _id: "6",
            data_format: "MAF",
            experimental_strategy: "WES",
            file_name: "dfci-9999.maf",
            number_of_samples: 276,
            file_size: 1234567,
            trial_name: "DFCI-9999",
            date_created: "2019-01-17T21:27:53.175496",
            gs_uri: "test_uri",
            download_link: "download_url",
            fastq_properties: null
        }
    ]);
});

test("Adds option to options list if it's not there", () => {
    expect(changeOption(["DFCI-9999"], "DFCI-1234")).toEqual([
        "DFCI-9999",
        "DFCI-1234"
    ]);
});

test("Removes option from options list if it's already there", () => {
    expect(changeOption(["DFCI-9999", "DFCI-1234"], "DFCI-1234")).toEqual([
        "DFCI-9999"
    ]);
});