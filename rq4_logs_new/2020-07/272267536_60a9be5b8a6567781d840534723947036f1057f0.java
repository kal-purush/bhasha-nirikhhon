package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.relationaldb.model.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ParallelSerializerTest {

    private static Logger logger = LoggerFactory.getLogger(ParallelSerializerTest.class);

    @Mock
    private DBModelSerializer serializer;

    @Test
    public void testExecuteParallelSerializer() throws Exception {
        ForkJoinPool threadPool = new ForkJoinPool(2);

        CountDownLatch taskCounter = new CountDownLatch(1);
        ParallelSerializer<DBModelSerializer> parallelSerializer = new ParallelSerializer<>(this.serializer, taskCounter);

        List<Table> dbEntities = this.getDBEntities();
        when(this.serializer.getLoadSteps()).thenReturn(this.getLoadCommands());
        when(this.serializer.assemble()).thenReturn(dbEntities);

        threadPool.execute(parallelSerializer);

        taskCounter.await();

        verify(this.serializer, times(1)).getLoadSteps();
        verify(this.serializer, times(1)).assemble();
        verify(this.serializer, times(1)).serialize(dbEntities);
    }

    /**
     * Create a dummy list of load commands (all the same) to be executed
     * by the ParallelSerializer
     * @return
     */
    private List<LoadCommand> getLoadCommands(){
        List<LoadCommand> commands = new ArrayList<>();

        for(int i=0; i < 10; i++){

            commands.add(new LoadCommand() {
                @Override
                public void execute() {
                    int a = 0;
                    for(int x = 1000000; x > 0; x--){
                        a += x;
                    }
                    logger.debug("Executed in different threads");
                }
            });
        }

        return commands;
    }

    private List<Table> getDBEntities(){
        List<Table> tables = new ArrayList<>();

        tables.add(new Table("schemaA", "TableA"));
        tables.add(new Table("schemaB", "TableB"));

        return tables;
    }
}