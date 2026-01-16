from mlcroissant import Dataset

ds = Dataset(jsonld="https://huggingface.co/api/datasets/jung1230/patient_info_and_summary/croissant")
records = ds.records("default")