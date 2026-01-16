package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.InMemoryTestDBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;
import org.mockito.Mock;

public class BaseSerializerTest {

    protected final String rootPath = "root";
    protected final JacksonYamlSerializer textSerializer = new JacksonYamlSerializer();

    @Mock
    protected RelationalDBRepository repository;

    protected InMemoryTestDBModelFS createNewDBModelFS(){
        return new InMemoryTestDBModelFS(this.rootPath, this.textSerializer);
    }


}