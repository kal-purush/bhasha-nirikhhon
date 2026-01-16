import pandas as pd
from gprofiler import GProfiler
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib_venn import venn2, venn3
from scipy import stats
import statsmodels.stats.multitest as multi

data_treatment = {"cell_line":["BT474","MCF7","SKBR3"],
        "location":["/home/bdpitica/scratch/mRNA_isoform/liqa.output/results/BT474_results_file",
                    "/home/bdpitica/scratch/mRNA_isoform/liqa.output/results/MCF7_results_file",
                    "/home/bdpitica/scratch/mRNA_isoform/liqa.output/results/SKBR3_results_file"]}

# data_control = {"cell_line":["BT474_vs_SKBR3","BT474_vs_MCF7","SKBR3_vs_MCF7"],
#         "location":["/home/bdpitica/scratch/mRNA_isoform/scripts/10_60_liqa.out/liqa_10_q60.output/DAS.out/BT474_vs_SKBR3_results/BT474_vs_SKBR3_results_file",
#                     "/home/bdpitica/scratch/mRNA_isoform/scripts/10_60_liqa.out/liqa_10_q60.output/DAS.out/BT474_vs_MCF7_results/BT474_vs_MCF7_results_file",
#                     "/home/bdpitica/scratch/mRNA_isoform/scripts/10_60_liqa.out/liqa_10_q60.output/DAS.out/SKBR3_vs_MCF7_results/SKBR3_vs_MCF7_results_file"]}

test_type = "_treatment"

for data_tuple in [(data_treatment,"_treatment")]:
    data, test_type = data_tuple
    file_df = pd.DataFrame(data)

    df = pd.DataFrame()

    for index, row in file_df.iterrows():
        temp_df = pd.read_csv(row.location, sep="\t", header=None)
        temp_df.columns = ["gene_name",
                            "p-value"]
        temp_df["cell_line"] = row.cell_line

        df = pd.concat([df, temp_df])

    df = df.reset_index(drop=True)

    # Perform FDR correction on each combination of cell line and replicate
    for (cell_line), group in df.groupby(['cell_line']):
        p_values = group['p-value'].tolist()
        _, q_values, _, _ = multi.multipletests(p_values, alpha=0.05, method='fdr_bh',is_sorted=False, returnsorted=False)
        
        # Assign q-values to the corresponding rows in the DataFrame
        df.loc[group.index, 'q-value'] = q_values

    df = df.sort_values(by="p-value", ascending=True)
    df = df[df["q-value"] < 0.05]


    df = df.sort_values(by="q-value")


    df.to_csv(f"adj_p-values{test_type}.tsv",sep='\t')

    grouped_df = df.groupby('cell_line')['gene_name'].agg(list).reset_index()
    result_df = grouped_df.rename(columns={'gene_name': 'genes_in_cell_line'})
    grouped_df.to_csv(f"list_of_genes_{test_type}.csv",sep=',')

    # Find unique combinations of 'gene_name' and 'isoform_name'
    unique_combinations = df['gene_name'].drop_duplicates()


    # Get unique cell lines
    cell_lines = df['cell_line'].unique()


    # Create an empty DataFrame with unique_combinations as the index and cell_lines as columns
    result_df = pd.DataFrame(columns=cell_lines, index=unique_combinations)

    # Fill the new DataFrame with boolean values
    for idx, row in result_df.iterrows():
        gene_name = idx
        for cell_line in cell_lines:
            if len(df[(df['gene_name'] == gene_name) & (df['cell_line'] == cell_line)]) == 1:
                row[cell_line] = idx

    sets = [set(result_df[cell_line].dropna()) for cell_line in result_df]

    result_df.to_csv(f"genes_of_interest_{test_type}.tsv",sep='\t')

    venn3(sets, cell_lines)

    # Display the Venn diagram
    plt.show()
    plt.savefig(f"Venn_diagram_{test_type}.png",format='png')
    plt.close()

    # Find unique combinations of 'gene_name' and 'isoform_name'
    unique_combinations = df['gene_name'].drop_duplicates()

    # Get unique cell lines
    cell_lines = df['cell_line'].unique()


    # Create an empty DataFrame with unique_combinations as the index and cell_lines as columns
    result_df = pd.DataFrame(columns=cell_lines, index=unique_combinations)

    # Fill the new DataFrame with boolean values
    for idx, row in result_df.iterrows():
        gene_name = idx
        for cell_line in cell_lines:
            if len(df[(df['gene_name'] == gene_name) & (df['cell_line'] == cell_line)]) == 1:
                row[cell_line] = idx

    results_dict = result_df.to_dict()

    def filter_dict(data):
        if isinstance(data, dict):
            return {key: filter_dict(value) for key, value in data.items() if value is not None and not pd.isna(value)}
        else:
            return data

    filtered_dict = filter_dict(results_dict)

    def GO_enrichment_analysis(filtered_dict, organism, user_agent=None):
        gp = GProfiler(user_agent=user_agent, return_dataframe=True)
        sources = ["GO:MF","GO:CC","GO:BP","KEGG"]
        # Perform KEGG enrichment analysis
        result = gp.profile(organism=organism, query=filtered_dict,sources=sources)

        return result

    if __name__ == "__main__":
        organism = 'hsapiens'

        go_terms_df = GO_enrichment_analysis(filtered_dict, organism)

        
        go_terms_df.to_csv(f"go_terms{test_type}.tsv",sep='\t')

    import matplotlib.pyplot as plt
    import matplotlib as mpl
    from matplotlib import cm
    import seaborn as sns
    import textwrap

    cmap = mpl.cm.bwr_r
    norm = mpl.colors.Normalize(vmin = go_terms_df.p_value.min(), vmax = go_terms_df.p_value.max())

    mapper = cm.ScalarMappable(norm = norm, cmap = cm.bwr_r)

    # Convert the NumPy array to a list
    palette_list = mapper.to_rgba(go_terms_df.p_value.values).tolist()


    # Assuming 'go_terms_df', 'mapper', and other relevant variables are defined

    # Create the main plot on the left
    plt.figure(figsize=(8, 4))  # Adjust the figure size as needed
    gs = plt.GridSpec(1, 2, width_ratios=[2, 1])  # 1 row, 2 columns, width ratio 3:1

    ax1 = plt.subplot(gs[0])  # 1st subplot

    palette_list = mapper.to_rgba(go_terms_df.p_value.values).tolist()

    import matplotlib.pyplot as plt
    import matplotlib as mpl
    from matplotlib import cm
    import seaborn as sns
    import textwrap

    cmap = mpl.cm.bwr_r
    norm = mpl.colors.Normalize(vmin=go_terms_df.p_value.min(), vmax=go_terms_df.p_value.max())
    mapper = cm.ScalarMappable(norm=norm, cmap=cm.bwr_r)

    # Get unique sources
    unique_sources = go_terms_df['query'].unique()

    # Calculate the number of rows and columns based on the number of sources
    num_sources = len(unique_sources)
    num_cols = min(num_sources, 3)  # Adjust the number of columns as needed
    num_rows = (num_sources + num_cols - 1) // num_cols

    # Create a single figure with subplots
    fig, axs = plt.subplots(num_rows, num_cols, figsize=(12, 4 * num_rows))

    # Flatten the axs array in case it's a multi-dimensional array
    axs = np.ravel(axs)

    for i, source in enumerate(unique_sources):
        # Filter DataFrame for the current source
        source_df = go_terms_df[go_terms_df['query'] == source]

        # Plot on the current subplot
        ax = axs[i]
        sns.barplot(
            data=source_df,
            x='recall',
            y='name',
            palette=mapper.to_rgba(source_df['p_value']),
            edgecolor='black',
            linewidth=1.2,
            alpha=0.7,
            ax=ax
        )
        ax.set_yticklabels([textwrap.fill(e, 18) for e in source_df['name']],fontsize=6)
        ax.set_title(f'GO Analysis - {source}')

    # Add a colorbar to the last subplot
    cbar_ax = fig.add_axes([0.95, 0.15, 0.02, 0.7])  # Adjust the position and size as needed
    cbar = mpl.colorbar.ColorbarBase(cbar_ax, cmap=cmap, norm=norm, orientation='vertical')
    cbar.set_label('adj. p-value')
    cbar.set_ticks([go_terms_df['p_value'].min(), go_terms_df['p_value'].max()])
    cbar.set_ticklabels(['{:.3f}'.format(go_terms_df['p_value'].min()), '{:.3f}'.format(go_terms_df['p_value'].max())])

    # Adjust layout
    plt.tight_layout(rect=[0, 0, 0.9, 1])

    # Save the combined plot
    plt.savefig(f"GO_analysis{test_type}.png", format="png", bbox_inches="tight")
    plt.close()