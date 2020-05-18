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
package org.knime.cloud.aws.dynamodb.batchget;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.cloud.aws.dynamodb.settings.DynamoDBKeyColumnsSettings;
import org.knime.cloud.aws.dynamodb.settings.DynamoDBWriterSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;

/**
 * Settings for the DynamoDB Batch Delete node.
 * @author Alexander Fillbrunn, University of Konstanz
 */
final class DynamoDBBatchGetSettings extends DynamoDBWriterSettings {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DynamoDBBatchGetSettings.class);
    
    private static final String CFG_BATCH_SIZE = "batchSize";
    private static final String CFG_CONSISTENT_READ = "consistentRead";
    private static final String CFG_NAMES = "names";
    private static final String CFG_PROJECTION = "projection";
    
    private static final int DEFAULT_BATCH_SIZE = 100;
    
    private int m_batchSize = DEFAULT_BATCH_SIZE;

    private DynamoDBKeyColumnsSettings m_keyColumns = new DynamoDBKeyColumnsSettings();
    private Map<String, String> m_names = new LinkedHashMap<String, String>();
    private boolean m_consistentRead = false;
    private String m_projectionExpression = "";
    
    /**
     * @return an expression determining which attributes to retrieve
     */
    String getProjectionExpression() {
        return m_projectionExpression;
    }
    
    /**
     * @param projection an expression determining which attributes to retrieve
     */
    void setProjectionExpression(final String projection) {
        m_projectionExpression = projection;
    }
    
    /**
     * @return whether the read item state should be consistent
     */
    boolean isConsistentRead() {
        return m_consistentRead;
    }
    
    /**
     * @param consistentRead whether the read item state should be consistent
     */
    void setConsistentRead(final boolean consistentRead) {
        m_consistentRead = consistentRead;
    }
    
    /**
     * @return settings indicating which columns are used as keys when deleting items
     */
    DynamoDBKeyColumnsSettings getKeyColumns() {
        return m_keyColumns;
    }
    
    /**
     * @return the number of rows to be sent to DynamoDB in one batch. Maximum is 25.
     */
    int getBatchSize() {
        return m_batchSize;
    }
    
    /**
     * @param batchSize the number of rows to be sent to DynamoDB in one batch. Maximum is 25.
     */
    void setBatchSize(final int batchSize) {
        if (batchSize > 100) {
            throw new IllegalArgumentException("Maximum batch size is 100.");
        }
        m_batchSize = batchSize;
    }
    
    /**
     * 
     * @return a list of name placeholders for expressions
     */
    Map<String, String> getNames() {
        return m_names;
    }

    /**
     * 
     * @param names a list of name placeholders for expressions
     */
    void setNames(final Map<String, String> names) {
        m_names = names;
    }
    
    /**
     * Saves this settings object to node settings.
     * @param settings the node settings to save to
     */
    @Override
    public void saveSettings(final NodeSettingsWO settings) {
        super.saveSettings(settings);
        settings.addInt(CFG_BATCH_SIZE, m_batchSize);
        settings.addBoolean(CFG_CONSISTENT_READ, m_consistentRead);
        m_keyColumns.saveSettings(settings);
        settings.addString(CFG_PROJECTION, m_projectionExpression);
        
        Config names = settings.addConfig(CFG_NAMES);
        for (Entry<String, String> name : m_names.entrySet()) {
            names.addString(name.getKey(), name.getValue());
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
        m_batchSize = settings.getInt(CFG_BATCH_SIZE);
        m_consistentRead = settings.getBoolean(CFG_CONSISTENT_READ);
        m_keyColumns.loadSettings(settings);
        m_projectionExpression = settings.getString(CFG_PROJECTION);
        
        m_names = new LinkedHashMap<String, String>();
        Config names = settings.getConfig(CFG_NAMES);
        for (String key : names.keySet()) {
            m_names.put(key, names.getString(key));
        }
    }

    /**
     * Loads settings with defaults from node settings.
     * @param settings the node settings to load from
     */
    @Override
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        super.loadSettingsForDialog(settings);
        m_batchSize = settings.getInt(CFG_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        m_consistentRead = settings.getBoolean(CFG_CONSISTENT_READ, false);
        m_keyColumns.loadSettingsForDialog(settings);
        m_projectionExpression = settings.getString(CFG_PROJECTION, "");
        
        m_names = new LinkedHashMap<String, String>();
        if (settings.containsKey(CFG_NAMES)) {
            try {
                Config names = settings.getConfig(CFG_NAMES);
                for (String key : names.keySet()) {
                    m_names.put(key, names.getString(key));
                }
            } catch (InvalidSettingsException e) {
                // Should not happen as we only query existing keys
                LOGGER.error(e);
            }
        }
    }
}
