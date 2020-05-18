/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   1 Jul 2019 (Alexander): created
 */
package org.knime.cloud.aws.dynamodb.createtable;

import java.util.ArrayList;
import java.util.List;

import org.knime.cloud.aws.dynamodb.settings.DynamoDBTableSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;

import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.Tag;

/**
 * Settings for the DynamoDB Create Table node.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
final class DynamoDBCreateTableSettings extends DynamoDBTableSettings {
    
    private static final String CFG_HASH_KEY_NAME = "hashKeyName";
    private static final String CFG_HASH_KEY_TYPE = "hashKeyType";
    
    private static final String CFG_HAS_RANGE_KEY = "hasRangeKey";
    private static final String CFG_RANGE_KEY_NAME = "rangeKeyName";
    private static final String CFG_RANGE_KEY_TYPE = "rangeKeyType";
    
    private static final String CFG_BILLING_MODE = "billingMode";
    
    private static final String CFG_READ_UNITS = "readUnits";
    private static final String CFG_WRITE_UNITS = "writeUnits";
    
    private static final String CFG_TAGS = "tags";
    
    private static final String CFG_BLOCK_UNTIL_ACTIVE = "blockUntilActive";
    
    private static final String CFG_INDEXES = "indexes";
    
    private static final String CFG_SSE_ENABLED = "sseEnabled";
    private static final String CFG_KMS_MASTER_KEY = "kmsMasterKey";
    
    private static final String CFG_STREAMS_ENABLED = "streamsEnabled";
    private static final String CFG_STREAM_VIEW_TYPE = "streamViewType";
    
    private static final int DEFAULT_CAPACITY_UNITS = 5;
    
    private String m_hashKeyName = "";
    private ScalarAttributeType m_hashKeyType = ScalarAttributeType.S;
    
    private boolean m_hasRangeKey = false;
    private String m_rangeKeyName = "";
    private ScalarAttributeType m_rangeKeyType = ScalarAttributeType.N;
    
    private BillingMode m_billingMode = BillingMode.PROVISIONED;
    private int m_readUnits = DEFAULT_CAPACITY_UNITS;
    private int m_writeUnits = DEFAULT_CAPACITY_UNITS;
    
    private List<Tag> m_tags = new ArrayList<>();
    
    private boolean m_blockUntilActive = false;
    
    private List<IndexSettings> m_indexes = new ArrayList<>();
    
    private boolean m_sseEnabled = false;
    private String m_kmsMasterKey = "";
    
    private boolean m_streamsEnabled = false;
    private StreamViewType m_streamViewType = StreamViewType.NEW_AND_OLD_IMAGES;
    
    /**
     * @return if true, server-side encryption is enabled
     */
    boolean isSseEnabled() {
        return m_sseEnabled;
    }

    /**
     * @param sseEnabled if true, server-side encryption is enabled
     */
    void setSseEnabled(final boolean sseEnabled) {
        m_sseEnabled = sseEnabled;
    }

    /**
     * @return the KMS master key ID for SSE
     */
    String getKmsMasterKeyId() {
        return m_kmsMasterKey;
    }

    /**
     * @param kmsMasterKey the KMS master key ID for SSE
     */
    void setKmsMasterKeyId(final String kmsMasterKey) {
        m_kmsMasterKey = kmsMasterKey;
    }

    /**
     * @return true if streams are enabled on the new table
     */
    boolean isStreamsEnabled() {
        return m_streamsEnabled;
    }

    /**
     * @param streamsEnabled true if streams are enabled on the new table
     */
    void setStreamsEnabled(final boolean streamsEnabled) {
        m_streamsEnabled = streamsEnabled;
    }

    /**
     * @return the attributes to include in the stream
     */
    StreamViewType getStreamViewType() {
        return m_streamViewType;
    }

    /**
     * @param streamViewType the attributes to include in the stream
     */
    void setStreamViewType(final StreamViewType streamViewType) {
        m_streamViewType = streamViewType;
    }

    /**
     * @return the indexes on the table
     */
    List<IndexSettings> getIndexes() {
        return m_indexes;
    }
    
    /**
     * @param indexes the indexes on the table
     */
    void setIndexes(final List<IndexSettings> indexes) {
        m_indexes = indexes;
    }
    
    /**
     * @return how the user is charged for requests to the table
     */
    BillingMode getBillingMode() {
        return m_billingMode;
    }
    
    /**
     * @param billingMode how the user is charged for requests to the table
     */
    void setBillingMode(final BillingMode billingMode) {
        m_billingMode = billingMode;
    }
    
    /**
     * @return whether the node should block execution until the table is created
     */
    boolean isBlockUntilActive() {
        return m_blockUntilActive;
    }

    /**
     * @param blockUntilActive whether the node should block execution until the table is created
     */
    void setBlockUntilActive(final boolean blockUntilActive) {
        m_blockUntilActive = blockUntilActive;
    }

    /**
     * @return a list of tag names and values
     */
    List<Tag> getTags() {
        return m_tags;
    }
    
    /**
     * @param tags a list of tag names and values
     */
    void setTags(final List<Tag> tags) {
        m_tags = tags;
    }
    
    /**
     * @return the name of the hash key attribute
     */
    String getHashKeyName() {
        return m_hashKeyName;
    }
    
    /**
     * @param hashKeyName the name of the hash key attribute
     */
    void setHashKeyName(final String hashKeyName) {
        m_hashKeyName = hashKeyName;
    }
    
    /**
     * @return the type of the hash key attribute
     */
    ScalarAttributeType getHashKeyType() {
        return m_hashKeyType;
    }
    
    /**
     * @param hashKeyType the type of the hash key attribute
     */
    void setHashKeyType(final ScalarAttributeType hashKeyType) {
        m_hashKeyType = hashKeyType;
    }
    
    /**
     * @return true if the new table has a range key attribute
     */
    boolean hasRangeKey() {
        return m_hasRangeKey;
    }
    
    /**
     * @param hasRangeKey true if the new table has a range key attribute
     */
    void setHasRangeKey(final boolean hasRangeKey) {
        m_hasRangeKey = hasRangeKey;
    }
    
    /**
     * @return the name of the range key attribute
     */
    String getRangeKeyName() {
        return m_rangeKeyName;
    }
    
    /**
     * @param rangeKeyName the name of the range key attribute
     */
    void setRangeKeyName(final String rangeKeyName) {
        m_rangeKeyName = rangeKeyName;
    }
    
    /**
     * @return the type of the range key attribute
     */
    ScalarAttributeType getRangeKeyType() {
        return m_rangeKeyType;
    }
    
    /**
     * @param rangeKeyType the type of the range key attribute
     */
    void setRangeKeyType(final ScalarAttributeType rangeKeyType) {
        m_rangeKeyType = rangeKeyType;
    }
    
    /**
     * @return the number of provisioned read units for the table
     */
    int getReadUnits() {
        return m_readUnits;
    }
    
    /**
     * @param readUnits the number of provisioned read units for the table
     */
    void setReadUnits(final int readUnits) {
        m_readUnits = readUnits;
    }
    
    /**
     * @return the number of provisioned write units for the table
     */
    int getWriteUnits() {
        return m_writeUnits;
    }
    
    /**
     * @param writeUnits the number of provisioned write units for the table
     */
    void setWriteUnits(final int writeUnits) {
        m_writeUnits = writeUnits;
    }
    
    /**
     * Saves this settings object to node settings.
     * @param settings the node settings to save to
     */
    @Override
    public void saveSettings(final NodeSettingsWO settings) {
        super.saveSettings(settings);
        settings.addString(CFG_HASH_KEY_NAME, m_hashKeyName);
        settings.addString(CFG_HASH_KEY_TYPE, m_hashKeyType.toString());
        
        settings.addBoolean(CFG_HAS_RANGE_KEY, m_hasRangeKey);
        settings.addString(CFG_RANGE_KEY_NAME, m_rangeKeyName);
        settings.addString(CFG_RANGE_KEY_TYPE, m_rangeKeyType.toString());
        
        settings.addString(CFG_BILLING_MODE, m_billingMode.toString());
        settings.addInt(CFG_READ_UNITS, m_readUnits);
        settings.addInt(CFG_WRITE_UNITS, m_writeUnits);
        
        settings.addBoolean(CFG_SSE_ENABLED, m_sseEnabled);
        settings.addString(CFG_KMS_MASTER_KEY, m_kmsMasterKey);
        
        settings.addBoolean(CFG_STREAMS_ENABLED, m_streamsEnabled);
        settings.addString(CFG_STREAM_VIEW_TYPE, m_streamViewType.toString());
        
        Config tags = settings.addConfig(CFG_TAGS);
        for (Tag tag : m_tags) {
            tags.addString(tag.key(), tag.value());
        }
        settings.addBoolean(CFG_BLOCK_UNTIL_ACTIVE, m_blockUntilActive);
        Config indexes = settings.addConfig(CFG_INDEXES);
        int idxCounter = 0;
        for (IndexSettings idx : m_indexes) {
            Config cfg = indexes.addConfig(String.format("Index_%s:%d", idx.getName(), idxCounter++));
            idx.saveToConfig(cfg);
        }
    }

    /**
     * Loads settings from node settings.
     * @param settings the node settings to load from
     * @throws InvalidSettingsException if the settings cannot be loaded
     */
    @Override
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettings(settings);
        m_hashKeyName = settings.getString(CFG_HASH_KEY_NAME);
        m_hashKeyType = ScalarAttributeType.valueOf(settings.getString(CFG_HASH_KEY_TYPE));
        
        m_hasRangeKey = settings.getBoolean(CFG_HAS_RANGE_KEY);
        m_rangeKeyName = settings.getString(CFG_RANGE_KEY_NAME);
        m_rangeKeyType = ScalarAttributeType.valueOf(settings.getString(CFG_RANGE_KEY_TYPE));
        
        m_billingMode = BillingMode.valueOf(settings.getString(CFG_BILLING_MODE));
        m_readUnits = settings.getInt(CFG_READ_UNITS);
        m_writeUnits = settings.getInt(CFG_WRITE_UNITS);
        m_blockUntilActive = settings.getBoolean(CFG_BLOCK_UNTIL_ACTIVE);
        
        m_sseEnabled = settings.getBoolean(CFG_SSE_ENABLED);
        m_kmsMasterKey = settings.getString(CFG_KMS_MASTER_KEY);
        
        m_streamsEnabled = settings.getBoolean(CFG_STREAMS_ENABLED);
        m_streamViewType = StreamViewType.valueOf(settings.getString(CFG_STREAM_VIEW_TYPE));
        
        m_tags.clear();
        Config tags = settings.getConfig(CFG_TAGS);
        for (String key : tags.keySet()) {
            m_tags.add(Tag.builder().key(key).value(tags.getString(key)).build());
        }
        
        m_indexes.clear();
        Config indexes = settings.getConfig(CFG_INDEXES);
        for (String key : indexes.keySet()) {
            IndexSettings idx = new IndexSettings();
            idx.loadFromConfig(indexes.getConfig(key));
            m_indexes.add(idx);
        }
    }

    /**
     * Loads settings with defaults from node settings.
     * @param settings the node settings to load from
     */
    @Override
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        super.loadSettingsForDialog(settings);
        m_hashKeyName = settings.getString(CFG_HASH_KEY_NAME, "");
        m_hashKeyType = ScalarAttributeType.valueOf(
                settings.getString(CFG_HASH_KEY_TYPE, ScalarAttributeType.S.toString()));
        
        m_hasRangeKey = settings.getBoolean(CFG_HAS_RANGE_KEY, false);
        m_rangeKeyName = settings.getString(CFG_RANGE_KEY_NAME, "");
        m_rangeKeyType = ScalarAttributeType.valueOf(
                settings.getString(CFG_RANGE_KEY_TYPE, ScalarAttributeType.N.toString()));
        
        m_billingMode = BillingMode.valueOf(settings.getString(CFG_BILLING_MODE, BillingMode.PROVISIONED.toString()));
        m_readUnits = settings.getInt(CFG_READ_UNITS, DEFAULT_CAPACITY_UNITS);
        m_writeUnits = settings.getInt(CFG_WRITE_UNITS, DEFAULT_CAPACITY_UNITS);
        m_blockUntilActive = settings.getBoolean(CFG_BLOCK_UNTIL_ACTIVE, false);
        
        m_sseEnabled = settings.getBoolean(CFG_SSE_ENABLED, false);
        m_kmsMasterKey = settings.getString(CFG_KMS_MASTER_KEY, "");
        
        m_streamsEnabled = settings.getBoolean(CFG_STREAMS_ENABLED, false);
        m_streamViewType = StreamViewType.valueOf(
                settings.getString(CFG_STREAM_VIEW_TYPE, StreamViewType.NEW_AND_OLD_IMAGES.toString()));
        
        m_tags.clear();
        Config tags = null;
        try {
            tags = settings.getConfig(CFG_TAGS);
        } catch (InvalidSettingsException e) {
            // We don't do anything, tags just stay empty
        }
        if (tags != null) {
            for (String key : tags.keySet()) {
                try {
                    m_tags.add(Tag.builder().key(key).value(tags.getString(key)).build());
                } catch (InvalidSettingsException e) {
                    // We ignore this tag
                }
            }
        }
        
        m_indexes.clear();
        Config indexes = null;
        try {
            indexes = settings.getConfig(CFG_INDEXES);
        } catch (InvalidSettingsException e) {
            // We don't do anything, indexes just stay empty
        }
        if (indexes != null) {
            for (String key : indexes.keySet()) {
                IndexSettings idx = new IndexSettings();
                try {
                    idx.loadFromConfig(indexes.getConfig(key));
                    m_indexes.add(idx);
                } catch (InvalidSettingsException e) {
                    // We ignore this index
                }
            }
        }
    }
}
