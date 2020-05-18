package org.knime.cloud.aws.dynamodb;

/**
 * Error messages and other constants for DynamoDB nodes.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public final class NodeConstants {
    
    private NodeConstants() { }
    
    /** Error message for missing tables with a single formatting arg for the table name. **/
    public static final String TABLE_MISSING_ERROR = "The given table \"%s\" does not exist.";
    
    /** Error message for missing tables or indices with two formatting args for the table name and the index name. **/
    public static final String TABLE_OR_INDEX_MISSING_ERROR = "The given table \"%s\" or index \"%s\" does not exist.";

    /** Error message for insufficient throughput capacity. **/
    public static final String THROUGHPUT_ERROR =
            "The table does not have sufficent throughput capacity for this operation.";
}
