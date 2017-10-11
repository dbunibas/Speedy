package speedy.model.database;

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

    private String hashString() {
        return table + "." + tuple.getOid();
    }

    @Override
    public int hashCode() {
        return hashString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final TupleWithTable other = (TupleWithTable) obj;
        return other.hashString().equals(this.hashString());
    }

    @Override
    public String toString() {
        return table + "." + tuple;
    }

}
