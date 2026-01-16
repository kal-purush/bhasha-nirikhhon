
export class GetNoteById {
  constructor(private noteRepository: KnexPgNoteRepository) { }

  async execute(id: number) {
    return await this.noteRepository.findById(id)
  }
}