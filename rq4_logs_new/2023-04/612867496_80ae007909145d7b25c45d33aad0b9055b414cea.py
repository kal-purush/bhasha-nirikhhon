import yaml
import pandas as pd
import ast


def generate():
    # Load configuration from YAML file
    with open("config.yml", encoding='utf-8', mode='r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.Loader)

        # Read paths from configuration
        read_path = cfg['generate']['read_path']
        author_output_path = cfg['generate']['author_output_path']
        publish_output_path = cfg['generate']['publish_output_path']
        publication_output_path = cfg['generate']['publication_output_path']

    # Read data from CSV file
    data = pd.read_csv(read_path, engine='pyarrow')

    # Create dataframe for publication by dropping columns
    df_publication = data.drop(['','author'],axis=1)
    df_publication = df_publication.rename(columns={'key':'PubKey'})

    # Create dataframe for publications and authors
    df_publish_rows = [[row['key'], author] for _, row in data[['key','author']].iterrows() for author in ast.literal_eval(row['author'])]
    df_publish = pd.DataFrame(df_publish_rows, columns=['PubKey','AuthorName'])
    authors = set(df_publish['AuthorName'].unique())
    df_authors = pd.DataFrame(authors,columns=['Name'])

    # Write dataframes to CSV files
    df_publication.to_csv(publication_output_path)
    df_authors.to_csv(author_output_path)
    df_publish.to_csv(publish_output_path)


if __name__ == '__main__':
    generate()


    




