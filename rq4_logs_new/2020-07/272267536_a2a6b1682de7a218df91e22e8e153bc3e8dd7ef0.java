package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.DBModelSerializer;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;

import java.net.MalformedURLException;
import java.util.List;

public interface DatabaseSerializerFactory {

    RelationalDBRepository createRDBRepository(String jdbcUrl, String username, String password);

    DBModelFS createDBModelFS(String rootPathPerEnv);

    VersionControlSystem createVCSController(String repositoryUrl, String username, String password, String baseBranch)
            throws MalformedURLException;

    List<DBModelSerializer> createDBObjectsSerializers(
            String environmentName, String schema, RelationalDBRepository repository, DBModelFS dbModelFS);
}