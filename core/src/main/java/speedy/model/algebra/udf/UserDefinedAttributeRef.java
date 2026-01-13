package speedy.model.algebra.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import speedy.model.database.TableAlias;
import speedy.model.database.VirtualAttributeRef;

public class UserDefinedAttributeRef extends VirtualAttributeRef {

    private static Logger logger = LoggerFactory.getLogger(UserDefinedAttributeRef.class);

    private IUserDefinedFunction userDefinedFunction;

    public UserDefinedAttributeRef(IUserDefinedFunction userDefinedFunction, TableAlias tableAlias, String name, String type) {
        super(tableAlias, name, type);
        this.userDefinedFunction = userDefinedFunction;
    }

    public IUserDefinedFunction getUserDefinedFunction() {
        return userDefinedFunction;
    }
}
