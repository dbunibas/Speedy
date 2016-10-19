package speedy.model.database;

public class Attribute {

    private String tableName;
    private String name;
    private String type;
    private Boolean nullable;

    public Attribute(String tableName, String name, String type) {
        this.tableName = tableName;
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getTableName() {
        return tableName;
    }

    public String getType() {
        return type;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public String toString() {
        return "Attribute{" + "tableName=" + tableName + ", name=" + name + ", type=" + type + '}';
    }

    
}
