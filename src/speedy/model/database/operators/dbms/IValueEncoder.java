package speedy.model.database.operators.dbms;


public interface IValueEncoder {
    
    public String encode(String original);
    public String decode(String encoded);

    public void prepareForEncoding();
    public void closeEncoding();
    
    public void prepareForDecoding();
    public void closeDecoding();
    
    public void waitingForEnding();
}
