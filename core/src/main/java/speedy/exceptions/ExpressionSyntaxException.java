package speedy.exceptions;

public class ExpressionSyntaxException extends RuntimeException {
    
    public ExpressionSyntaxException() {
        super();
    }
    
    public ExpressionSyntaxException(String message) {
        super(message);
    }

}
