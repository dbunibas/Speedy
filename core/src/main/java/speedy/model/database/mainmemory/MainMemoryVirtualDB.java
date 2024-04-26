package speedy.model.database.mainmemory;

import speedy.SpeedyConstants;
import speedy.model.database.mainmemory.datasource.DataSource;
import speedy.model.database.mainmemory.datasource.ForeignKeyConstraint;
import speedy.model.database.mainmemory.datasource.KeyConstraint;
import speedy.model.database.mainmemory.paths.PathExpression;
import java.util.ArrayList;
import java.util.List;
import speedy.model.database.AttributeRef;
import speedy.model.database.ForeignKey;
import speedy.model.database.IDatabase;
import speedy.model.database.ITable;
import speedy.model.database.Key;

public class MainMemoryVirtualDB implements IDatabase {

    private MainMemoryDB originalDB;
    private DataSource dataSourceForSchema;
    private List<MainMemoryVirtualTable> tables;

    public MainMemoryVirtualDB(MainMemoryDB originalDB, DataSource dataSource, List<MainMemoryVirtualTable> tables) {
        this.originalDB = originalDB;
        this.dataSourceForSchema = dataSource;
        this.tables = tables;
    }

    public String getName() {
        return dataSourceForSchema.getSchema().getLabel();
    }

    public DataSource getDataSource() {
        return dataSourceForSchema;
    }

    public MainMemoryDB getOriginalDB() {
        return originalDB;
    }

    public void addTable(ITable table) {
        this.tables.add((MainMemoryVirtualTable) table);
    }

    public List<String> getTableNames() {
        List<String> result = new ArrayList<String>();
        for (MainMemoryVirtualTable virtualTable : tables) {
            result.add(virtualTable.getName());
        }
        return result;
//        INode schema = dataSourceForSchema.getSchema();
//        List<String> result = new ArrayList<String>();
//        for (INode setNode : schema.getChildren()) {
//            result.add(setNode.getLabel());
//        }
//        return result;
    }

    public List<Key> getKeys() {
        List<Key> result = new ArrayList<Key>();
        for (KeyConstraint keyConstraint : dataSourceForSchema.getKeyConstraints()) {
            List<AttributeRef> attributeRefs = extractPaths(keyConstraint.getKeyPaths());
            result.add(new Key(attributeRefs, keyConstraint.isPrimaryKey()));
        }
        return result;
    }

    private List<AttributeRef> extractPaths(List<PathExpression> pathExpressions) {
        List<AttributeRef> attributeRefs = new ArrayList<AttributeRef>();
        for (PathExpression pathExpression : pathExpressions) {
            String tableName = pathExpression.getPathSteps().get(1);
            String attributeName = pathExpression.getPathSteps().get(3);
            AttributeRef attributeRef = new AttributeRef(tableName, attributeName);
            attributeRefs.add(attributeRef);
        }
        return attributeRefs;
    }

    public List<Key> getKeys(String table) {
        List<Key> result = new ArrayList<Key>();
        for (Key key : getKeys()) {
            String tableName = key.getAttributes().get(0).getTableName();
            if (tableName.equals(table)) {
                result.add(key);
            }
        }
        return result;
    }

    @Override
    public List<Key> getPrimaryKeys() {
        List<Key> result = new ArrayList<Key>();
        for (KeyConstraint keyConstraint : dataSourceForSchema.getKeyConstraints()) {
            List<AttributeRef> attributeRefs = extractPaths(keyConstraint.getKeyPaths());
            if (keyConstraint.isPrimaryKey()) {
                result.add(new Key(attributeRefs, true));
            }
        }
        return result;
    }

    @Override
    public Key getPrimaryKey(String table) {
        for (Key key : getPrimaryKeys()) {
            String tableName = key.getAttributes().get(0).getTableName();
            if (tableName.equals(table)) {
                return key;
            }
        }
        return null;
    }

    public List<ForeignKey> getForeignKeys() {
        List<ForeignKey> result = new ArrayList<ForeignKey>();
        for (ForeignKeyConstraint foreignKeyConstraint : dataSourceForSchema.getForeignKeyConstraints()) {
            List<AttributeRef> keyAttributes = extractPaths(foreignKeyConstraint.getKeyConstraint().getKeyPaths());
            List<AttributeRef> refAttributes = extractPaths(foreignKeyConstraint.getForeignKeyPaths());
            ForeignKey foreignKey = new ForeignKey(keyAttributes, refAttributes);
            result.add(foreignKey);
        }
        return result;
    }

    public List<ForeignKey> getForeignKeys(String table) {
        List<ForeignKey> result = new ArrayList<ForeignKey>();
        for (ForeignKey foreignKey : getForeignKeys()) {
            String tableName = foreignKey.getRefAttributes().get(0).getTableName();
            if (tableName.equals(table)) {
                result.add(foreignKey);
            }
        }
        return result;
    }

    public ITable getTable(String name) {
        for (MainMemoryVirtualTable table : tables) {
            if (table.getName().equalsIgnoreCase(name)) {
                return table;
            }
        }
        throw new IllegalArgumentException("Unable to find table " + name + " in db " + tables);
    }

    public ITable getFirstTable() {
        return getTable(getTableNames().get(0));
    }

    public long getSize() {
        return this.originalDB.getSize();
    }

    public MainMemoryVirtualDB clone() {
        try {
            MainMemoryVirtualDB clone = (MainMemoryVirtualDB) super.clone();
            clone.dataSourceForSchema = this.dataSourceForSchema.clone();
            clone.tables = new ArrayList<MainMemoryVirtualTable>(this.tables);
            return clone;
        } catch (CloneNotSupportedException ex) {
            return null;
        }
    }

    public String printSchema() {
        StringBuilder result = new StringBuilder();
        result.append("Schema: ").append(getName()).append(" {\n");
        for (MainMemoryVirtualTable table : tables) {
            result.append(table.printSchema(SpeedyConstants.INDENT));
        }
//        for (String tableName : getTableNames()) {
//            ITable table = getTable(tableName);
//            result.append(table.printSchema(SpeedyConstants.INDENT));
//        }
        if (!getKeys().isEmpty()) {
            result.append(SpeedyConstants.INDENT).append("--------------- Keys: ---------------\n");
            for (Key key : getKeys()) {
                result.append(SpeedyConstants.INDENT).append(key).append("\n");
            }
        }
        if (!getForeignKeys().isEmpty()) {
            result.append(SpeedyConstants.INDENT).append("----------- Foreign Keys: -----------\n");
            for (ForeignKey foreignKey : getForeignKeys()) {
                result.append(SpeedyConstants.INDENT).append(foreignKey).append("\n");
            }
        }
        result.append("}\n");
        return result.toString();
    }

    public String printInstances() {
        return printInstances(false);
    }

    public String printInstances(boolean sort) {
        StringBuilder result = new StringBuilder();
        result.append("Tables: ").append(getName()).append(" {\n");
//        for (String tableName : getTableNames()) {
//            ITable table = getTable(tableName);
        for (MainMemoryVirtualTable table : tables) {
            if (sort) {
                result.append(table.toStringWithSort(SpeedyConstants.INDENT));
            } else {
                result.append(table.toString(SpeedyConstants.INDENT));
            }
        }
        result.append("}\n");
        return result.toString();
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(printSchema());
        result.append(printInstances());
        return result.toString();
    }
}
