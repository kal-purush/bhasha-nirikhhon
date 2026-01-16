# bhasha-nirikhhon
![Bhasha Nirikhhon - ICSE 2026 Artifact](banner.png)
Bhasha Nirikhhon is the accompanying artifact for the ICSE 2026 paper
“Write in English, Nobody Understands Your Language Here”: A Study of Non-English Trends in Open-Source Repositories.  
This repository contains the data (RQ3 & RQ4), scripts, Jupyter notebooks, and results required to reproduce and understand the analyses reported in the paper.

This README collects reproducibility instructions for RQ1–RQ6.

---

## Quick start (minimal steps)

Prerequisites
- Python 3.13.5
- From repo root:
```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements_paper.txt
```

Run the scripts (examples below assume scripts/ folder)
```bash
# RQ1
python scripts/RQ1.py

# RQ2
python scripts/RQ2.py

# RQ3
python scripts/RQ3.py

# RQ4
python scripts/RQ4.py

# RQ5
python scripts/RQ5.py

# RQ6 figures
python scripts/RQ6_figure10.py
python scripts/RQ6_figure11.py
python scripts/RQ6_figure12.py
python scripts/RQ6_figure13.py
```

Notes:
- RQ1, RQ2, and RQ5 operate on precomputed CSV summaries (no GH Archive preprocessing required).
- RQ3 and RQ4 require parsed-code JSONs and language-detection segment files (details below).
- RQ6 scripts expect prepared CSVs with repository-level metrics (details below).

---

## RQ1 & RQ2 — monthly language summaries (no preprocessing required)

Input files (place in repository root or update script paths)
- language_counts_by_month_2015.csv … language_counts_by_month_2025.csv

Each monthly CSV must contain at least:
- `date` (e.g., `2015-01`)
- `language` (string)
- `count` (integer)

What they do
- RQ1 (`scripts/RQ1.py`): computes percent non-English messages per month and saves `RQ1.pdf`.
- RQ2 (`scripts/RQ2.py`): selects top-10 non-Western-European non-English languages and saves a stacked percent time-series `RQ2.pdf`.

Configuration
- Change YEARS, EXCLUDED_LANGUAGES, and output filenames at the top of `scripts/RQ1.py` and `scripts/RQ2.py`.

---

## RQ3 — code-level non-English statistics (scripts/RQ3.py)

Required inputs
1. `lang_detection_result*` files (glob: `./lang_detection_result*`) — language detection segments. Segments must end with `-> <language>, <confidence>`. RQ3 uses segments with confidence >= 0.9 to build a mapping of text segment → language.
2. Parsed code JSONs under: `code_parser_data_rq2/{YYYY}-{MM}/`  
   - Expected JSON keys: `comments`, `docstrings`, `identifiers`, `variables`, `classes`, `functions`, `literals`.

What it does
- For each key (comments, docstrings, classes, identifiers, functions, literals), it marks files as non-English if they contain non-ASCII items that are not mapped to English in the detection mapping. Aggregates per-month/year and plots trends.

Run
```bash
python scripts/RQ3.py
```

Outputs
- Interactive trend plot; add `plt.savefig(...)` in the script to save PDF(s).

Notes
- If detection files are missing, classification falls back to an ASCII heuristic (may misclassify).
- Edit years/months and confidence threshold inside `scripts/RQ3.py` as needed.

---

## RQ4 — sampled code-language analysis (scripts/RQ4.py)

Required inputs
1. `lang_detection_result*` files (same format as RQ3).
2. `rq4_logs_new/<YYYY>-<MM>/*` — folder structure used to infer year-month and map to parsed JSON names.
3. Parsed JSONs under: `code_parser_data_rq4/{year}-{month}/{file_base}_{year}_{month}.json`  
   - Keys expected: `comments`, `docstrings`, `identifiers`, `variables`, `classes`, `functions`, `literals`.

What it does
- Samples up to N files per language/month (N=100 default), uses the high-confidence mapping from lang-detection files to detect non-English items in code elements, aggregates per-language/year, and outputs one PDF per key:
  - `non_english_by_language_comments.pdf`
  - `non_english_by_language_classes.pdf`
  - `non_english_by_language_identifiers.pdf`
  - `non_english_by_language_functions.pdf`
  - `non_english_by_language_literals.pdf`

Run
```bash
python scripts/RQ4.py
```

Notes
- The script infers programming language from file extension; update `get_language_from_filename()` if your naming differs.
- Adjust sampling size and confidence threshold in the script to trade runtime vs. coverage.

---

## RQ5 — repository language-classification counts (scripts/RQ5.py)

Required input
- `repo_language_classification1.csv` (repo root or adjust path)

Expected columns
- `classification` — the category/name for each repo (e.g., "Mostly English", "Non-English", "Mixed" or your schema).

What it does
- Counts the number of repositories per classification and saves `RQ5.pdf`.

Run
```bash
python scripts/RQ5.py
```

Tip
- To sort bars by count, sort the DataFrame inside `scripts/RQ5.py` before plotting.

---

## RQ6 — repository-level metrics and comparisons (scripts/RQ6_figure10.py / 11 / 12 / 13)

RQ6 produces a set of figure scripts that visualize repository-level measures across contribution-size buckets and classification labels.

Scripts and required inputs
- `scripts/RQ6_figure10.py`
  - Input: `total_comments_data.csv`
  - Expected columns: `repo_id`, `total_comments`, `label` (english/mixed/non_english), `push_bin`
  - Output: interactive boxplot; the `plt.savefig(...)` line is present but commented — enable to save `total_comments_per_repo_repro.pdf`.

- `scripts/RQ6_figure11.py`
  - Input: `total_contributors_data.csv`
  - Expected columns: `repo_id`, `contributors_count`, `label`, `push_count`
  - The script bins `push_count` into buckets and produces `total_contributors_per_repo.pdf`.

- `scripts/RQ6_figure12.py`
  - Input: `total_star_data.csv`
  - Expected columns: `repo_id`, `stargazers_count`, `label`, `push_count`
  - The script bins `push_count` and produces an interactive boxplot (saving is optional).

- `scripts/RQ6_figure13.py`
  - Input: `common_with_duration_push_classification.csv`
  - Expected columns: `duration_seconds`, `push_count`, `classification` (normalized to english/mixed/non-english)
  - Produces `issue_resolution_time_by_classification_and_repo_activity.pdf` (script saves the PDF by default).

Common notes for RQ6 scripts
- All RQ6 plots use contribution-size buckets (ordered):
  ```
  ['<5','<10','<50','<100','<500','<1k','<5k','<10k','<25k','<50k','<100k','<500k','1M+']
  ```
  or use pd.cut with the bins used in the scripts.
- Many plots use a log y-axis to handle skewed distributions — ensure zero or negative values are handled (filter or add epsilon).
- Verify the column names in your CSVs match the scripts; edit the script top sections if necessary.
- To save interactive figures, make sure `plt.savefig(...)` is enabled and the script has write permission in the working directory.

Run RQ6 scripts
```bash
python scripts/RQ6_figure10.py
python scripts/RQ6_figure11.py
python scripts/RQ6_figure12.py
python scripts/RQ6_figure13.py
```

---

## Optional: low-level GH Archive preprocessing (process_issues.py)

- `process_issues.py` parses GH Archive hourly gz files and produces daily outputs under `resul_files/`. This script needs:
  - `data/gharchive_{YEAR}_{MM}/` with hourly `{YYYY}-{MM}-{DD}-{HOUR}.json.gz`
  - `id_files/combined_non_english_ids_{YYYY}_{MM}_{DD}.csv`
  - `id_files/combined_english_ids_{YYYY}_{MM}_{DD}.csv`
  - `resul_files/` for outputs
- NOTE: RQ1/RQ2/RQ5 do not require running this if monthly summary CSVs are already provided.

---

## Where to change behavior / configuration

- `scripts/RQ1.py` — `YEARS`, `EXCLUDED_LANGUAGES`, `OUTPUT_FNAME`
- `scripts/RQ2.py` — `YEARS`, `EXCLUDED_LANGUAGES`, `OUTPUT_FNAME`
- `scripts/RQ3.py` / `scripts/RQ4.py` — input directories, confidence threshold, sampling, years/months
- `scripts/RQ5.py` — input filename and plotting options
- `scripts/RQ6_figure*.py` — input filenames, bucket definitions, and save behavior

---

## Troubleshooting / common issues

- Missing CSVs: ensure the expected CSV filenames are in the repo root or update script paths.
- Lang-detection files missing (RQ3/RQ4): provide `lang_detection_result*` files in working directory.
- Parsed JSONs missing (RQ3/RQ4): ensure `code_parser_data_rq2/` and `code_parser_data_rq4/` exist with parsed JSON outputs.
- Log-scale plots and zeros: if a dataset contains zeros, add a small epsilon or filter zeros before plotting on log scales.
- Performance: RQ3/RQ4 iterate many files — consider sampling, running on machines with sufficient RAM/CPU, or adding multiprocessing.

---

## Contributing & Contact

See `docs/CONTRIBUTING.md` for contribution guidelines.

Maintainer: Masudul Hasan Masud Bhuiyan — masudul.bhuiyan [at] proton [dot] me  
Prefer contact via GitHub: [@kal-purush](https://github.com/kal-purush) or open an issue.

## Citation

```bibtex
@inproceedings{bhuiyan2025write,
	title        = {{``Write in English, Nobody Understands Your Language Here'': A Study of Non-English Trends in Open-Source Repositories}},
	author       = {Bhuiyan, Masudul Hasan Masud and Bala Kumar, Manish Kumar and Staicu, Cristian-Alexandru},
	year         = 2026,
	booktitle    = {Proceedings of the IEEE/ACM International Conference on Software Engineering (ICSE)},
	publisher    = {IEEE}
}
```
