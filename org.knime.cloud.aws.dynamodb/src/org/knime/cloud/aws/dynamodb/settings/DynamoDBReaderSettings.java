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
package org.knime.cloud.aws.dynamodb.settings;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for nodes reading from DynamoDB.
 * @author Alexander Fillbrunn, University of Konstanz
 */
public class DynamoDBReaderSettings extends DynamoDBTableSettings {

    private static final int UNLIMITED = 0;

    private static final String CFG_LIMIT = "limit";
    private static final String CFG_USE_INDEX = "useIndex";
    private static final String CFG_INDEX_NAME = "indexName";
    private static final String CFG_FILTER = "filter";
    private static final String CFG_PROJECTION = "projection";

    private static final String CFG_CONSISTENT_READ = "consistentRead";
    
    private static final String CFG_FLOW_VAR_CONSUMED_CAP_UNTS = "consumedCapUnitsFlowVar";
    
    private boolean m_consistentRead = false;
    private int m_limit = UNLIMITED;
    private boolean m_useIndex = false;
    private String m_indexName = "";
    private String m_filterExpr = "";
    private String m_projectionExpr = "";
    private DynamoDBPlaceholderSettings m_placeholderSettings = new DynamoDBPlaceholderSettings();
    private boolean m_flowVarConsumedCapUnits = false;
    
    /**
     * @return whether to use an index with the name given in {@link #getIndexName() getIndexName}
     */
    public boolean getUseIndex() {
        return m_useIndex;
    }
    
    /**
     * @param useIndex whether to use an index with the name given in {@link #getIndexName() getIndexName}
     */
    public void setUseIndex(final boolean useIndex) {
        m_useIndex = useIndex;
    }
    
    /**
     * @return the maximum number of items to retrieve
     */
    public int getLimit() {
        return m_limit;
    }

    /**
     * @param limit the maximum number of items to retrieve
     */
    public void setLimit(final int limit) {
        m_limit = limit;
    }

    /**
     * @return the name of the index to use or empty if no index should be used
     */
    public String getIndexName() {
        return m_indexName;
    }

    /**
     * @param indexName the name of the index to use or empty if no index should be used
     */
    public void setIndexName(final String indexName) {
        m_indexName = indexName;
    }

    /**
     * @return if true, a flow variable with consumed capacity units is published after execution
     */
    public boolean publishConsumedCapUnits() {
        return m_flowVarConsumedCapUnits;
    }
    
    /**
     * @param flowVarConsumedCapUnits if true, a flow variable with
     * consumed capacity units is published after execution.
     */
    public void setPublishConsumedCapUnits(final boolean flowVarConsumedCapUnits) {
        m_flowVarConsumedCapUnits = flowVarConsumedCapUnits;
    }
    
    /**
     * @return whether the query should perform consistent reads (more expensive)
     */
    public boolean isConsistentRead() {
        return m_consistentRead;
    }

    /**
     * @param consistentRead if true, only consistent reads are performed
     */
    public void setConsistentRead(final boolean consistentRead) {
        m_consistentRead = consistentRead;
    }

    /**
     * A filter expression for further filtering of query results,
     * apart from the hash and range key.
     * @see <a href="https://docs.aws.amazon.com/en_en/amazondynamodb/latest/developerguide
/Query.html#Query.FilterExpression">DynamoDB documentation</a>
     * @return the filter expression
     */
    public String getFilterExpr() {
        return m_filterExpr;
    }

    /**
     * A filter expression for further filtering of query results,
     * apart from the hash and range key.
     * @see <a href="https://docs.aws.amazon.com/en_en/amazondynamodb/latest/developerguide
/Query.html#Query.FilterExpression">DynamoDB documentation</a>
     * @param filterExpr the filter expression
     */
    public void setFilterExpr(final String filterExpr) {
        m_filterExpr = filterExpr;
    }

    /**
     * @return a string of comma separated column names
     * and name placeholders determining which columns to return
     */
    public String getProjectionExpr() {
        return m_projectionExpr;
    }
    
    /**
     * 
     * @param projectionExpr a string of comma separated column names
     * and name placeholders determining which columns to return
     */
    public void setProjectionExpr(final String projectionExpr) {
        m_projectionExpr = projectionExpr;
    }
    
    /**
     * @return Settings for expression placeholders
     */
    public DynamoDBPlaceholderSettings getPlaceholderSettings() {
        return m_placeholderSettings;
    }
    
    /**
     * Saves this settings object to node settings.
     * @param settings the node settings to save to
     */
    @Override
    public void saveSettings(final NodeSettingsWO settings) {
        super.saveSettings(settings);
        
        settings.addInt(CFG_LIMIT, m_limit);
        settings.addBoolean(CFG_USE_INDEX, m_useIndex);
        settings.addString(CFG_INDEX_NAME, m_indexName);
        settings.addBoolean(CFG_FLOW_VAR_CONSUMED_CAP_UNTS, m_flowVarConsumedCapUnits);
        settings.addBoolean(CFG_CONSISTENT_READ, m_consistentRead);
        settings.addString(CFG_FILTER, m_filterExpr);
        settings.addString(CFG_PROJECTION, m_projectionExpr);
        m_placeholderSettings.saveSettings(settings);
    }

    /**
     * Loads settings from node settings.
     * @param settings the node settings to load from
     * @throws InvalidSettingsException if the settings cannot be loaded
     */
    @Override
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettings(settings);
        
        m_limit = settings.getInt(CFG_LIMIT);
        m_useIndex = settings.getBoolean(CFG_USE_INDEX);
        m_indexName = settings.getString(CFG_INDEX_NAME);
        m_flowVarConsumedCapUnits = settings.getBoolean(CFG_FLOW_VAR_CONSUMED_CAP_UNTS);
        m_consistentRead = settings.getBoolean(CFG_CONSISTENT_READ);
        m_filterExpr = settings.getString(CFG_FILTER);
        m_projectionExpr = settings.getString(CFG_PROJECTION);
        m_placeholderSettings.loadSettings(settings);
    }

    /**
     * Loads settings with defaults from node settings.
     * @param settings the node settings to load from
     */
    @Override
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        super.loadSettingsForDialog(settings);
        
        m_limit = settings.getInt(CFG_LIMIT, UNLIMITED);
        m_useIndex = settings.getBoolean(CFG_USE_INDEX, false);
        m_indexName = settings.getString(CFG_INDEX_NAME, "");
        m_flowVarConsumedCapUnits = settings.getBoolean(CFG_FLOW_VAR_CONSUMED_CAP_UNTS, false);
        m_consistentRead = settings.getBoolean(CFG_CONSISTENT_READ, false);
        m_filterExpr = settings.getString(CFG_FILTER, "");
        m_projectionExpr = settings.getString(CFG_PROJECTION, "");
        m_placeholderSettings.loadSettingsForDialog(settings);
    }
}
