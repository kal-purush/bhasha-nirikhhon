package br.lassal.dbvcs.tatubola.relationaldb.model;

public class IndexDummyBuilder {

    public Index createTestIndex(String schema, int id, boolean unique, boolean columnsOutOfOrder){
        String indexName = "Dummy_Idx0" + id;
        String targetTable = "Table_001";
        String indexType = "SOME_TYPE";

        Index idx = new Index(schema, indexName, schema, targetTable, indexType, unique);

        if(columnsOutOfOrder){
            idx.addColumn(this.createTestIndexColumn(3));
            idx.addColumn(this.createTestIndexColumn(5));
            idx.addColumn(this.createTestIndexColumn(1));
            idx.addColumn(this.createTestIndexColumn(2));
            idx.addColumn(this.createTestIndexColumn(4));
        }
        else{
            for(int i =1; i < 6; i++){
                idx.addColumn(this.createTestIndexColumn(i));
            }
        }


        return idx;
    }

    private IndexColumn createTestIndexColumn(int seqId){
        String columnName = "IDX_COL_" + seqId;
        ColumnOrder order = seqId % 2 == 0 ? ColumnOrder.ASC : ColumnOrder.DESC;
        IndexColumn col = new IndexColumn(columnName, seqId, order);

        return col;
    }

}