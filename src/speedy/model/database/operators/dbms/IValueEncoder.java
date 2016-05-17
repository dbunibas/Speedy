package speedy.model.database.operators.dbms;


public interface IValueEncoder {
    
    public String encode(String original);
    public String decode(String encoded);

}
