package org.knime.cloud.aws.dynamodb.createtable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.Config;

import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Settings for a DynamoDB index on a table.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class IndexSettings {

    /**
     * The type of a DynamoDB index.
     * @author Alexander Fillbrunn, University of Konstanz
     *
     */
    public enum IndexType {
        /** A local index. **/
        LOCAL,
        /** A global index. **/
        GLOBAL
    }
    
    private static final int DEFAULT_CAPACITY_UNITS = 5;
    
    private static final String CFG_TYPE = "type";
    private static final String CFG_NAME = "name";
    private static final String CFG_HASH_KEY_NAME = "hashKeyName";
    private static final String CFG_HASH_KEY_TYPE = "hashKeyType";
    private static final String CFG_HAS_RANGE_KEY = "hasRangeKey";
    private static final String CFG_RANGE_KEY_NAME = "rangeKeyName";
    private static final String CFG_RANGE_KEY_TYPE = "rangeKeyType";
    private static final String CFG_READ_UNITS = "readUnits";
    private static final String CFG_WRITE_UNITS = "writeUnits";
    private static final String CFG_PROJECTION_TYPE = "projectionType";
    private static final String CFG_PROJECTION = "projection";
    
    private IndexType m_type = IndexType.LOCAL;
    private String m_name = "";
    private String m_hashKeyName = "";
    private ScalarAttributeType m_hashKeyType = ScalarAttributeType.S;
    
    private boolean m_hasRangeKey = true;
    private String m_rangeKeyName = "";
    private ScalarAttributeType m_rangeKeyType = ScalarAttributeType.N;
    
    private int m_readUnits = DEFAULT_CAPACITY_UNITS;
    private int m_writeUnits = DEFAULT_CAPACITY_UNITS;
    
    private ProjectionType m_projectionType = ProjectionType.ALL;
    private List<String> m_projection = new ArrayList<>();
    
    /**
     * @return whether this index is local or global
     */
    public IndexType getType() {
        return m_type;
    }
    
    /**
     * @param type whether this index is local or global
     */
    public void setType(final IndexType type) {
        m_type = type;
    }
    
    /**
     * @return the index name
     */
    public String getName() {
        return m_name;
    }
    
    /**
     * @param name the index name
     */
    public void setName(final String name) {
        m_name = name;
    }
    
    /**
     * @return the name of the hash key attribute
     */
    public String getHashKeyName() {
        return m_hashKeyName;
    }
    
    /**
     * @param hashKeyName the name of the hash key attribute
     */
    public void setHashKeyName(final String hashKeyName) {
        m_hashKeyName = hashKeyName;
    }
    
    /**
     * @return the scalar type of the hash key
     */
    public ScalarAttributeType getHashKeyType() {
        return m_hashKeyType;
    }
    
    /**
     * @param hashKeyType the scalar type of the hash key
     */
    public void setHashKeyType(final ScalarAttributeType hashKeyType) {
        m_hashKeyType = hashKeyType;
    }
    
    /**
     * @return whether the index has a range key or not.
     * Local indexes need a range key as their hash key is the
     * hash key of the parent table.
     */
    public boolean hasRangeKey() {
        return m_hasRangeKey;
    }
    
    /**
     * @param hasRangeKey whether the index has a range key or not.
     * Local indexes need a range key as their hash key is the
     * hash key of the parent table.
     */
    public void setHasRangeKey(final boolean hasRangeKey) {
        m_hasRangeKey = hasRangeKey;
    }
    
    /**
     * @return the name of the range key attribute
     */
    public String getRangeKeyName() {
        return m_rangeKeyName;
    }
    
    /**
     * @param rangeKeyName the name of the range key attribute
     */
    public void setRangeKeyName(final String rangeKeyName) {
        m_rangeKeyName = rangeKeyName;
    }
    
    /**
     * @return the scalar type of the range key
     */
    public ScalarAttributeType getRangeKeyType() {
        return m_rangeKeyType;
    }
    
    /**
     * @param rangeKeyType the scalar type of the hash key
     */
    public void setRangeKeyType(final ScalarAttributeType rangeKeyType) {
        m_rangeKeyType = rangeKeyType;
    }
    
    /**
     * @return the number of read units provisioned for the index
     */
    public int getReadUnits() {
        return m_readUnits;
    }
    
    /**
     * @param readUnits the number of read units provisioned for the index
     */
    public void setReadUnits(final int readUnits) {
        m_readUnits = readUnits;
    }
    
    /**
     * @return the number of write units provisioned for the index
     */
    public int getWriteUnits() {
        return m_writeUnits;
    }
    
    /**
     * @param writeUnits the number of write units provisioned for the index
     */
    public void setWriteUnits(final int writeUnits) {
        m_writeUnits = writeUnits;
    }
    
    /**
     * @return projection type determining which attributes are included in the index
     */
    public ProjectionType getProjectionType() {
        return m_projectionType;
    }
    
    /**
     * @param projectionType projection type determining which attributes are included in the index
     */
    public void setProjectionType(final ProjectionType projectionType) {
        m_projectionType = projectionType;
    }
    
    /**
     * @return included attributes in the index if the projection type is <code>INCLUDE</code>
     */
    public List<String> getProjection() {
        return m_projection;
    }
    
    /**
     * @param projection included attributes (only necessary if the projection type is <code>INCLUDE</code>)
     */
    public void setProjection(final List<String> projection) {
        m_projection = projection;
    }
    
    /**
     * Saves this <code>IndexSettings</code> object to the config object.
     * @param cfg the config object to save the index settings in.
     */
    public void saveToConfig(final Config cfg) {
        cfg.addString(CFG_TYPE, m_type.toString());
        cfg.addString(CFG_NAME, m_name);
        
        cfg.addString(CFG_HASH_KEY_NAME, m_hashKeyName);
        cfg.addString(CFG_HASH_KEY_TYPE, m_hashKeyType.toString());
        cfg.addBoolean(CFG_HAS_RANGE_KEY, m_hasRangeKey);
        cfg.addString(CFG_RANGE_KEY_NAME, m_rangeKeyName);
        cfg.addString(CFG_RANGE_KEY_TYPE, m_rangeKeyType.toString());
        
        cfg.addInt(CFG_READ_UNITS, m_readUnits);
        cfg.addInt(CFG_WRITE_UNITS, m_writeUnits);
        
        cfg.addString(CFG_PROJECTION_TYPE, m_projectionType.toString());
        cfg.addStringArray(CFG_PROJECTION, m_projection.toArray(new String[0]));
    }
    
    /**
     * Loads an <code>IndexSetting</code> from a config object.
     * @param cfg the config object containing information about the index
     * @throws InvalidSettingsException when the config object cannot be read
     */
    public void loadFromConfig(final Config cfg) throws InvalidSettingsException {
        m_type = IndexType.valueOf(cfg.getString(CFG_TYPE));
        m_name = cfg.getString(CFG_NAME);
        
        m_hashKeyName = cfg.getString(CFG_HASH_KEY_NAME);
        m_hashKeyType = ScalarAttributeType.valueOf(cfg.getString(CFG_HASH_KEY_TYPE));
        m_hasRangeKey = cfg.getBoolean(CFG_HAS_RANGE_KEY);
        m_rangeKeyName = cfg.getString(CFG_RANGE_KEY_NAME);
        m_rangeKeyType = ScalarAttributeType.valueOf(cfg.getString(CFG_RANGE_KEY_TYPE));
        
        m_readUnits = cfg.getInt(CFG_READ_UNITS);
        m_writeUnits = cfg.getInt(CFG_WRITE_UNITS);
        
        m_projectionType = ProjectionType.valueOf(cfg.getString(CFG_PROJECTION_TYPE));
        m_projection = Arrays.asList(cfg.getStringArray(CFG_PROJECTION));
    }
}
