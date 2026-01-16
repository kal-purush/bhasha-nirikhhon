import {PaperRepository} from "../../domain/repositories/paper.repository.ts";
import {Paper} from "../../domain/entities/paper.entity.ts";
import {Client} from "https://deno.land/x/mysql@v2.10.2/mod.ts";

export class PaperRepositoryImpl implements PaperRepository {
  private client: Client;

  constructor(private readonly connectionString: string) {
    this.client = new Client();
  }

  async connect() {
    await this.client.connect(this.connectionString);
  }

  async disconnect() {
    await this.client.close();
  }

  async findByDoi(doi: string): Promise<Paper | undefined> {
    const result = await this.client.execute(
      "SELECT * FROM papers WHERE doi = ?",
      [doi]
    );
    return result.rows?.[0] as Paper | undefined;
  }

  async save(paper: Paper): Promise<void> {
    await this.client.execute(
      "INSERT INTO papers (title, authors, publication_date, publisher, publication_name, doi, naid, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [
        paper.title,
        paper.authors,
        paper.publicationDate,
        paper.publisher,
        paper.publicationName,
        paper.doi,
        paper.naid,
        paper.url,
      ]
    );
  }
}