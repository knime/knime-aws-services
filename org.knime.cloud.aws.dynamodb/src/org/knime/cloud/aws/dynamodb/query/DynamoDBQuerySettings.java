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
package org.knime.cloud.aws.dynamodb.query;

import org.knime.cloud.aws.dynamodb.settings.DynamoDBReaderSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Settings for a node that queries DynamoDB.
 * @author Alexander Fillbrunn, University of Konstanz
 */
final class DynamoDBQuerySettings extends DynamoDBReaderSettings {

    private static final String CFG_HASH_KEY_NAME = "hashKeyName";
    private static final String CFG_HASH_KEY_VALUE = "hashKeyValue";
    private static final String CFG_HASH_KEY_TYPE = "hashKeyType";
    
    private static final String CFG_USE_RANGE_KEY = "useRangeKey";
    private static final String CFG_RANGE_KEY_NAME = "rangeKeyName";
    private static final String CFG_RANGE_KEY_VALUE1 = "rangeKeyValue1";
    private static final String CFG_RANGE_KEY_VALUE2 = "rangeKeyValue2";
    private static final String CFG_RANGE_KEY_TYPE = "rangeKeyType";
    private static final String CFG_RANGE_KEY_OPERATOR = "rangeKeyOperator";
    private static final String CFG_SCAN_INDEX_FORWARD = "scanIndexForward";
    
    private String m_hashKeyName = "";
    private String m_hashKeyValue = "";
    private ScalarAttributeType m_hashKeyType = ScalarAttributeType.S;
    
    private boolean m_useRangeKey = false;
    private String m_rangeKeyName = "";
    private String m_rangeKeyValue1 = "";
    private String m_rangeKeyValue2 = "";
    private ScalarAttributeType m_rangeKeyType = ScalarAttributeType.N;
    private String m_rangeKeyOperator = "=";
    private boolean m_scanIndexForward = true;

    /**
     * @return in which direction to scan the index or table
     */
    boolean scanIndexForward() {
        return m_scanIndexForward;
    }
    
    /**
     * @param scanIndexForward in which direction to scan the index or table
     */
    void setScanIndexForward(final boolean scanIndexForward) {
        m_scanIndexForward = scanIndexForward;
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
     * @return the value a hash key must match to be a result of the query.
     */
    String getHashKeyValue() {
        return m_hashKeyValue;
    }

    /**
     * @param hashKeyValue the value a hash key must match to be a result of the query.
     */
    void setHashKeyValue(final String hashKeyValue) {
        m_hashKeyValue = hashKeyValue;
    }

    /**
     * @return the type of the hash key attribute
     */
    ScalarAttributeType getHashKeyType() {
        return m_hashKeyType;
    }

    /**
     * 
     * @param hashKeyType the type of the hash key attribute
     */
    void setHashKeyType(final ScalarAttributeType hashKeyType) {
        m_hashKeyType = hashKeyType;
    }

    /**
     * @return true if a range key condition should be evaluated
     */
    boolean isUseRangeKey() {
        return m_useRangeKey;
    }

    /**
     * 
     * @param useRangeKey true if a range key condition should be evaluated
     */
    void setUseRangeKey(final boolean useRangeKey) {
        m_useRangeKey = useRangeKey;
    }

    /**
     * @return name of the range key attribute
     */
    String getRangeKeyName() {
        return m_rangeKeyName;
    }

    /**
     * @param rangeKeyName name of the range key attribute
     */
    void setRangeKeyName(final String rangeKeyName) {
        m_rangeKeyName = rangeKeyName;
    }

    /**
     * Returns the first value of the condition for the range key.
     * If the operator is not BETWEEN, this is the only value that is necessary.
     * @return string representation of the value to check for
     */
    String getRangeKeyValue1() {
        return m_rangeKeyValue1;
    }

    /**
     * Sets the first value of the condition for the range key.
     * If the operator is not BETWEEN, this is the only value that is necessary.
     * @param rangeKeyValue string representation of the value to check the range key for
     */
    void setRangeKeyValue1(final String rangeKeyValue) {
        m_rangeKeyValue1 = rangeKeyValue;
    }
    
    /**
     * Returns the second value of the condition for the range key.
     * Only necessary if the operator is BETWEEN.
     * @return the upper limit of the BETWEEN clause
     */
    String getRangeKeyValue2() {
        return m_rangeKeyValue2;
    }

    /**
     * Sets the second value of the condition for the range key.
     * Only necessary if the operator is BETWEEN.
     * @param rangeKeyValue the upper limit of the BETWEEN clause
     */
    void setRangeKeyValue2(final String rangeKeyValue) {
        m_rangeKeyValue2 = rangeKeyValue;
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
     * @return the operator to use for the condition on the range key
     */
    String getRangeKeyOperator() {
        return m_rangeKeyOperator;
    }

    /**
     * 
     * @param rangeKeyOperator the operator to use for the condition on the range key
     */
    void setRangeKeyOperator(final String rangeKeyOperator) {
        m_rangeKeyOperator = rangeKeyOperator;
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
        settings.addString(CFG_HASH_KEY_VALUE, m_hashKeyValue);
        
        settings.addBoolean(CFG_USE_RANGE_KEY, m_useRangeKey);
        settings.addString(CFG_RANGE_KEY_NAME, m_rangeKeyName);
        settings.addString(CFG_RANGE_KEY_TYPE, m_rangeKeyType.toString());
        settings.addString(CFG_RANGE_KEY_VALUE1, m_rangeKeyValue1);
        settings.addString(CFG_RANGE_KEY_VALUE2, m_rangeKeyValue2);
        settings.addString(CFG_RANGE_KEY_OPERATOR, m_rangeKeyOperator);
        settings.addBoolean(CFG_SCAN_INDEX_FORWARD, m_scanIndexForward);
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
        m_hashKeyValue = settings.getString(CFG_HASH_KEY_VALUE);
        m_hashKeyType = ScalarAttributeType.valueOf(settings.getString(CFG_HASH_KEY_TYPE));
        
        m_useRangeKey = settings.getBoolean(CFG_USE_RANGE_KEY);
        m_rangeKeyName = settings.getString(CFG_RANGE_KEY_NAME);
        m_rangeKeyValue1 = settings.getString(CFG_RANGE_KEY_VALUE1);
        m_rangeKeyValue2 = settings.getString(CFG_RANGE_KEY_VALUE2);
        m_rangeKeyType = ScalarAttributeType.valueOf(settings.getString(CFG_RANGE_KEY_TYPE));
        m_rangeKeyOperator = settings.getString(CFG_RANGE_KEY_OPERATOR);
        m_scanIndexForward = settings.getBoolean(CFG_SCAN_INDEX_FORWARD);
    }

    /**
     * Loads settings with defaults from node settings.
     * @param settings the node settings to load from
     */
    @Override
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        super.loadSettingsForDialog(settings);
        
        m_hashKeyName = settings.getString(CFG_HASH_KEY_NAME, "");
        m_hashKeyValue = settings.getString(CFG_HASH_KEY_VALUE, "");
        m_hashKeyType = ScalarAttributeType.valueOf(
                settings.getString(CFG_HASH_KEY_TYPE, ScalarAttributeType.S.toString()));
        
        m_useRangeKey = settings.getBoolean(CFG_USE_RANGE_KEY, false);
        m_rangeKeyName = settings.getString(CFG_RANGE_KEY_NAME, "");
        m_rangeKeyValue1 = settings.getString(CFG_RANGE_KEY_VALUE1, "");
        m_rangeKeyValue2 = settings.getString(CFG_RANGE_KEY_VALUE2, "");
        m_rangeKeyType = ScalarAttributeType.valueOf(
                settings.getString(CFG_RANGE_KEY_TYPE, ScalarAttributeType.N.toString()));
        m_rangeKeyOperator = settings.getString(CFG_RANGE_KEY_OPERATOR, "=");
        m_scanIndexForward = settings.getBoolean(CFG_SCAN_INDEX_FORWARD, true);
    }
    
}
