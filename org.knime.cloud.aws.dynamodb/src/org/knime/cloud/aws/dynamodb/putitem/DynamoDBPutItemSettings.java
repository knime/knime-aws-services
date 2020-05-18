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
package org.knime.cloud.aws.dynamodb.putitem;

import org.knime.cloud.aws.dynamodb.settings.DynamoDBPlaceholderSettings;
import org.knime.cloud.aws.dynamodb.settings.DynamoDBWriterSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

/**
 * Settings for the DynamoDB Put Item node.
 * @author Alexander Fillbrunn, University of Konstanz
 *
 */
final class DynamoDBPutItemSettings extends DynamoDBWriterSettings {
    
    private static final String CFG_CONDITION_EXPRESSION = "conditionExpression";
    private static final String CFG_RETURN_VALUE = "returnValue";
    
    private DynamoDBPlaceholderSettings m_placeholders = new DynamoDBPlaceholderSettings();
    
    private String m_conditionExpression = "";
    private ReturnValue m_returnValue = ReturnValue.NONE;
    
    /**
     * @return an expression describing which conditions have to be met for the item to be put
     */
    String getConditionExpression() {
        return m_conditionExpression;
    }

    /**
     * @param conditionExpression an expression describing which conditions have to be met for the item to be put
     */
    void setConditionExpression(final String conditionExpression) {
        m_conditionExpression = conditionExpression;
    }

    /**
     * @return which values to return after the update
     */
    ReturnValue getReturnValue() {
        return m_returnValue;
    }

    /**
     * @param returnValue which values to return after the put operation. Only valid values are NONE or ALL_OLD.
     */
    void setReturnValue(final ReturnValue returnValue) {
        if (returnValue != ReturnValue.NONE && returnValue != ReturnValue.ALL_OLD) {
            throw new IllegalArgumentException("For a put item operation only NONE "
                    + "or ALL_OLD are valid values for the return value.");
        }
        m_returnValue = returnValue;
    }

    /**
     * @return settings for placeholder names and values for the expressions
     */
    DynamoDBPlaceholderSettings getPlaceholders() {
        return m_placeholders;
    }
    
    /**
     * Saves this settings object to node settings.
     * @param settings the node settings to save to
     */
    @Override
    public void saveSettings(final NodeSettingsWO settings) {
        super.saveSettings(settings);
        m_placeholders.saveSettings(settings);
        settings.addString(CFG_CONDITION_EXPRESSION, m_conditionExpression);
        settings.addString(CFG_RETURN_VALUE, m_returnValue.toString());
    }

    /**
     * Loads settings from node settings.
     * @param settings the node settings to load from
     * @throws InvalidSettingsException if the settings cannot be loaded
     */
    @Override
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettings(settings);
        m_placeholders.loadSettings(settings);
        m_conditionExpression = settings.getString(CFG_CONDITION_EXPRESSION);
        m_returnValue = ReturnValue.valueOf(settings.getString(CFG_RETURN_VALUE));
    }

    /**
     * Loads settings with defaults from node settings.
     * @param settings the node settings to load from
     */
    @Override
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        super.loadSettingsForDialog(settings);
        m_placeholders.loadSettingsForDialog(settings);
        m_conditionExpression = settings.getString(CFG_CONDITION_EXPRESSION, "");
        m_returnValue = ReturnValue.valueOf(settings.getString(CFG_RETURN_VALUE, ReturnValue.NONE.toString()));
    }
}
