package org.knime.cloud.aws.dynamodb.settings;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Class for storing settings of hash and range key columns in a KNIME table.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
public class DynamoDBKeyColumnsSettings {

    private static final String CFG_HASH_KEY_COLUMN = "hashKeyColumn";
    private static final String CFG_RANGE_KEY_COLUMN = "rangeKeyColumn";
    private static final String CFG_HASH_KEY_BINARY = "hashKeyBinary";
    private static final String CFG_RANGE_KEY_BINARY = "rangeKeyBinary";
    
    private String m_hashKeyColumn = null;
    private String m_rangeKeyColumn = null;
    private boolean m_hashKeyBinary = false;
    private boolean m_rangeKeyBinary = false;
    
    /**
     * @return true if the string hash key should be interpreted as base64
     */
    public boolean isHashKeyBinary() {
        return m_hashKeyBinary;
    }

    /**
     * @param hashKeyBinary true if the string hash key should be interpreted as base64
     */
    public void setHashKeyBinary(final boolean hashKeyBinary) {
        m_hashKeyBinary = hashKeyBinary;
    }

    /**
     * @return true if the string range key should be interpreted as base64
     */
    public boolean isRangeKeyBinary() {
        return m_rangeKeyBinary;
    }

    /**
     * @param rangeKeyBinary true if the string range key should be interpreted as base64
     */
    public void setRangeKeyBinary(final boolean rangeKeyBinary) {
        m_rangeKeyBinary = rangeKeyBinary;
    }

    /**
     * @return the column containing the hash key of the items to be deleted
     */
    public String getHashKeyColumn() {
        return m_hashKeyColumn;
    }

    /**
     * @param hashKeyColumn the column containing the hash key of the items to be deleted
     */
    public void setHashKeyColumn(final String hashKeyColumn) {
        m_hashKeyColumn = hashKeyColumn;
    }

    /**
     * @return the column containing the range key
     * of the items to be deleted or null if no range key exists
     */
    public String getRangeKeyColumn() {
        return m_rangeKeyColumn;
    }

    /**
     * @param rangeKeyColumn the column containing the range key
     * of the items to be deleted or null if no range key exists
     */
    public void setRangeKeyColumn(final String rangeKeyColumn) {
        m_rangeKeyColumn = rangeKeyColumn;
    }

    /**
     * Saves this settings object to node settings.
     * @param settings the node settings to save to
     */
    public void saveSettings(final NodeSettingsWO settings) {
        settings.addString(CFG_HASH_KEY_COLUMN, m_hashKeyColumn);
        settings.addString(CFG_RANGE_KEY_COLUMN, m_rangeKeyColumn);
        settings.addBoolean(CFG_HASH_KEY_BINARY, m_hashKeyBinary);
        settings.addBoolean(CFG_RANGE_KEY_BINARY, m_rangeKeyBinary);
    }

    /**
     * Loads settings from node settings.
     * @param settings the node settings to load from
     * @throws InvalidSettingsException if the settings cannot be loaded
     */
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_hashKeyColumn = settings.getString(CFG_HASH_KEY_COLUMN);
        m_rangeKeyColumn = settings.getString(CFG_RANGE_KEY_COLUMN);
        m_hashKeyBinary = settings.getBoolean(CFG_HASH_KEY_BINARY);
        m_rangeKeyBinary = settings.getBoolean(CFG_RANGE_KEY_BINARY);
    }

    /**
     * Loads settings with defaults from node settings.
     * @param settings the node settings to load from
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        m_hashKeyColumn = settings.getString(CFG_HASH_KEY_COLUMN, null);
        m_rangeKeyColumn = settings.getString(CFG_RANGE_KEY_COLUMN, null);
        m_hashKeyBinary = settings.getBoolean(CFG_HASH_KEY_BINARY, false);
        m_rangeKeyBinary = settings.getBoolean(CFG_RANGE_KEY_BINARY, false);
    }
}
