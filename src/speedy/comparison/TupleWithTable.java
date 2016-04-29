package speedy.comparison;

import speedy.model.database.Tuple;

public class TupleWithTable {
    
    private String table;
    private Tuple tuple;

    public TupleWithTable(String table, Tuple tuple) {
        this.table = table;
        this.tuple = tuple;
    }

    public String getTable() {
        return table;
    }

    public Tuple getTuple() {
        return tuple;
    }

    @Override
    public String toString() {
        return table + "." + tuple;
    }
        
}
