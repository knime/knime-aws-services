package org.knime.cloud.aws.dynamodb;

/**
 * Holds results of a DynamoDB batch operation such as batch put, delete or get.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class BatchOperationResult {
    
    private int m_numUnprocessed = 0;
    private double m_consumedCapacity = 0.0;
    
    /**
     * Creates a new instance of <code>BatchOperationResult</code>
     * with 0 unprocessed items and 0 consumed capacity.
     */
    public BatchOperationResult() { }
    
    /**
     * Creates a new instance of <code>BatchOperationResult</code>
     * with the given number of unprocessed items and consumed capacity.
     * @param numUnprocessed the number of items not processed in the batch
     * @param consumedCapacity the consumed read and write capacity of the operation
     */
    public BatchOperationResult(final int numUnprocessed, final double consumedCapacity) {
        m_numUnprocessed = numUnprocessed;
        m_consumedCapacity = consumedCapacity;
    }

    /**
     * @return the number of items not processed in the batch
     */
    public int getNumUnprocessed() {
        return m_numUnprocessed;
    }

    /**
     * @param numUnprocessed the number of items not processed in the batch
     */
    public void setNumUnprocessed(final int numUnprocessed) {
        m_numUnprocessed = numUnprocessed;
    }

    /**
     * @return the consumed read and write capacity of the operation
     */
    public double getConsumedCapacity() {
        return m_consumedCapacity;
    }

    /**
     * @param consumedCapacity the consumed read and write capacity of the operation
     */
    public void setConsumedCapacity(final double consumedCapacity) {
        m_consumedCapacity = consumedCapacity;
    }
}
